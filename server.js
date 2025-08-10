require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');
const { ApifyClient } = require('apify-client');

const { fetchSiteText } = require('./webtext');
const { enrichWithLLM } = require('./llm');
const { detectFeatures, computeTechScore, subscores, tierFromScore } = require('./scoring');

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(cors({ origin: (process.env.CORS_ORIGIN || '').split(',').filter(Boolean) || true }));

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const apify = new ApifyClient({ token: process.env.APIFY_API_TOKEN });

const ACTORS = { places: 'compass/crawler-google-places' };

/** Simple concurrency helper (replaces p-limit) */
async function mapLimit(items, limit, iterator) {
  const results = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.max(1, limit) }, async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) break;
      results[idx] = await iterator(items[idx], idx);
    }
  });
  await Promise.all(workers);
  return results;
}

/** ======= TARGETING PRESETS (no pediatric) ======= **/
const SEARCH_PRESETS = {
  general: ["dentist","dental clinic","family dentist"],
  cosmetic: [
    "cosmetic dentist","veneers","smile makeover","esthetic dentist",
    "teeth whitening","invisalign dentist","smile design"
  ],
  implants: ["dental implants","all-on-4","full arch implants","implant dentist","teeth in a day"],
  aligners_ortho: ["invisalign dentist","clear aligners","orthodontist","braces","aligner therapy"],
  prosthodontics: ["prosthodontist","full mouth reconstruction","crowns and bridges","rehabilitation dentist"],
  digital: [
    "digital dentistry","CEREC","same day crown","intraoral scanner","3d printer","itero","cbct","digital workflow"
  ]
};

// Large DSO/chain names to skip when avoidChains=true
const DEFAULT_CHAINS = [
  "Aspen Dental","Western Dental","Pacific Dental","Heartland Dental",
  "Smile Direct Club","Ideal Dental","Bright Now Dental","DentalWorks",
  "Affordable Dentures","ClearChoice","Coast Dental","Great Expressions",
  "Aspire Dental","Midwest Dental","InterDent","MB2 Dental","Dental Care Alliance"
].map(s => s.toLowerCase());

const domainFrom = (url) => {
  if (!url) return null;
  try { return new URL(url).hostname.replace(/^www\./,'').toLowerCase(); } catch { return null; }
};

async function upsertLead(base) {
  const domain = domainFrom(base.website);
  const conds = [
    base.google_place_id ? `google_place_id.eq.${base.google_place_id}` : null,
    domain ? `domain.eq.${domain}` : null,
    base.phone ? `phone.eq.${base.phone}` : null
  ].filter(Boolean).join(',');

  let existing = null;
  if (conds) {
    const q = await supabase.from('dental_leads').select('id').or(conds).limit(1);
    existing = q.data?.[0] || null;
  }

  const payload = {
    google_place_id: base.google_place_id || null,
    name: base.name,
    address: base.address,
    domain: domain || null,
    city: base.city, state: base.state, postal_code: base.postal_code,
    latitude: base.latitude, longitude: base.longitude,
    phone: base.phone, website: base.website, email: base.email,
    rating: base.rating, review_count: base.review_count,
    categories: base.categories || [],
    opening_hours: base.opening_hours || {},
    temporarily_closed: !!base.temporarily_closed,
    permanently_closed: !!base.permanently_closed
  };

  if (existing) {
    await supabase.from('dental_leads').update(payload).eq('id', existing.id);
    return existing.id;
  } else {
    const { data, error } = await supabase.from('dental_leads').insert(payload).select('id').single();
    if (error) throw error;
    return data.id;
  }
}

async function saveTechMvp(lead_id, features, siteText, llm) {
  const row = {
    lead_id,
    technologies: features.technologies,
    has_online_scheduling: features.has_online_scheduling,
    has_patient_portal: features.has_patient_portal,
    has_text_reminders: features.has_text_reminders,
    has_digital_forms: features.has_digital_forms,
    has_online_payments: features.has_online_payments,
    has_virtual_consults: features.has_virtual_consults,
    has_advanced_imaging: features.has_advanced_imaging,
    website_text_excerpt: siteText ? siteText.slice(0, 1500) : null,
    llm_specialties: llm.specialties || [],
    llm_notes: llm.notes || null
  };
  const ex = await supabase.from('lead_tech_analysis').select('id').eq('lead_id', lead_id).limit(1);
  if (ex.data?.length) await supabase.from('lead_tech_analysis').update(row).eq('lead_id', lead_id);
  else await supabase.from('lead_tech_analysis').insert(row);
}

async function rescore(lead_id) {
  const { data: lead } = await supabase.from('dental_leads').select('*').eq('id', lead_id).single();
  const { data: tech } = await supabase.from('lead_tech_analysis').select('*').eq('lead_id', lead_id).single();

  const techScore = computeTechScore(tech || {});
  const specialties = tech?.llm_specialties || [];
  const specialtyBoost = (specialties.includes('cosmetic') || specialties.includes('aligners')) ? 60 :
                         (specialties.length ? 40 : 0);

  const ss = subscores({
    techScore,
    rating: Number(lead?.rating || 0),
    reviews: Number(lead?.review_count || 0),
    hasBooking: !!tech?.has_online_scheduling,
    specialtyBoost
  });

  const { tier, qual } = tierFromScore(ss.final);

  await supabase.from('dental_leads').update({
    tech_score: techScore,
    tech_tier: tier,
    investment_level: tier,
    qualification_status: qual,
    final_score: ss.final,
    final_score_explanation: tech?.llm_notes || null
  }).eq('id', lead_id);

  await supabase.from('lead_events').insert({ lead_id, event_type: 'rescored', payload: { ss, tier, qual } });
}

/** ============= Routes ============= **/
app.get('/api/health', (_, res) => res.json({ ok: true }));

// Generate: use actor().start so it returns immediately
app.post('/api/generate', async (req, res) => {
  const {
    location,
    maxResults = 40,
    preset = 'general',
    minRating = 3.8,
    minReviews = 10,
    avoidChains = true,
    includeKeywords = [],   // e.g. ["cerec","intraoral scanner","3d print","smile design"]
    excludeKeywords = []    // e.g. ["medicaid"]
  } = req.body || {};

  if (!location) return res.status(400).json({ error: 'location required' });

  try {
    const searches = SEARCH_PRESETS[preset] || SEARCH_PRESETS.general;

    const input = {
      searchStringsArray: searches,
      locationQuery: location,
      maxCrawledPlacesPerSearch: Math.max(1, Math.ceil(maxResults / searches.length)),
      includeWebsite: true,
      skipPlacesWithoutWebsite: true,
      additionalInfo: true,
      enrichPlaceWithBusinessLeads: true,
      minReviews,
      minRating
    };

    const run = await apify.actor(ACTORS.places).start(input, {
      webhooks: [{
        eventTypes: ['ACTOR.RUN.SUCCEEDED','ACTOR.RUN.FAILED'],
        requestUrl: `${process.env.PUBLIC_BASE_URL}/api/apify/webhook`
      }]
    });

    await supabase.from('lead_runs').insert({
      source: 'google_places',
      actor_id: ACTORS.places,
      run_id: run.id,
      status: run.status,
      meta: { location, maxResults, preset, minRating, minReviews, avoidChains, includeKeywords, excludeKeywords }
    });

    res.json({ ok: true, runId: run.id });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Robust webhook: resolve datasetId from multiple places; bail gracefully if missing
app.post('/api/apify/webhook', async (req, res) => {
  res.status(200).json({ received: true }); // ack immediately

  try {
    const body = req.body || {};
    const datasetRaw =
      body?.resource?.defaultDatasetId ||
      body?.defaultDatasetId ||
      body?.datasetId ||
      null;

    const datasetId = datasetRaw ? String(datasetRaw).split('/').pop() : null;
    const runId = body?.resource?.id || body?.id || null;

    if (!datasetId && !runId) {
      console.warn('webhook: no datasetId or runId in payload');
      return;
    }

    let finalDatasetId = datasetId;
    if (!finalDatasetId && runId) {
      // Fallback: fetch the run to get its defaultDatasetId
      const run = await apify.run(runId);
      finalDatasetId = run?.defaultDatasetId || null;
    }
    if (!finalDatasetId) {
      console.warn('webhook: unable to resolve datasetId');
      return;
    }

    const { items } = await apify.dataset(finalDatasetId).listItems({ limit: 1000 });

    // load run options for filtering
    const { data: runMetaRow } = await supabase
      .from('lead_runs').select('meta').eq('run_id', runId).single();
    const opts = runMetaRow?.meta || {};
    const avoidChains = opts.avoidChains ?? true;
    const includeKeywords = Array.isArray(opts.includeKeywords) ? opts.includeKeywords : [];
    const excludeKeywords = Array.isArray(opts.excludeKeywords) ? opts.excludeKeywords : [];

    await mapLimit(items, 3, async (item) => {
      const base = {
        google_place_id: item.placeId,
        name: item.title,
        address: item.address || item.streetAddress,
        city: item.city, state: item.state, postal_code: item.postalCode,
        latitude: item.location?.lat, longitude: item.location?.lng,
        phone: item.phone, website: item.website || item.url, email: item.email,
        rating: item.rating || item.stars, review_count: item.reviewsCount || item.reviews,
        categories: item.categories || [], opening_hours: item.openingHours,
        temporarily_closed: item.temporarilyClosed || false, permanently_closed: item.permanentlyClosed || false
      };

      // Skip big chains if requested
      if (avoidChains && DEFAULT_CHAINS.some(c => (base.name || '').toLowerCase().includes(c))) {
        return;
      }

      const id = await upsertLead(base);

      // Enrichment
      let siteText = '';
      if (base.website) siteText = await fetchSiteText(base.website);

      // Post-filters on homepage text (digital / cosmetic targeting)
      const low = (siteText || '').toLowerCase();
      if (includeKeywords.length && !includeKeywords.some(k => low.includes(String(k).toLowerCase()))) {
        return;
      }
      if (excludeKeywords.length && excludeKeywords.some(k => low.includes(String(k).toLowerCase()))) {
        return;
      }

      const features = detectFeatures(siteText);
      const llm = await enrichWithLLM({ siteText });

      await saveTechMvp(id, features, siteText, llm);
      await rescore(id);

      await supabase.from('lead_events').insert({ lead_id: id, event_type: 'created', payload: { runId, datasetId: finalDatasetId } });
    });

    await supabase.from('lead_runs').update({ status: 'succeeded', finished_at: new Date() }).eq('run_id', runId);
  } catch (e) {
    console.error('webhook', e);
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`API listening on :${PORT}`));
