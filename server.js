require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');
const { ApifyClient } = require('apify-client');
const pLimit = require('p-limit');

const { fetchSiteText } = require('./webtext');
const { enrichWithLLM } = require('./llm');
const { detectFeatures, computeTechScore, subscores, tierFromScore } = require('./scoring');

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(cors({ origin: (process.env.CORS_ORIGIN || '').split(',').filter(Boolean) || true }));

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const apify = new ApifyClient({ token: process.env.APIFY_API_TOKEN });

const ACTORS = { places: 'compass/crawler-google-places' };

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

/** Routes **/
app.get('/api/health', (_, res) => res.json({ ok: true }));

app.post('/api/generate', async (req, res) => {
  const { location, maxResults = 40 } = req.body || {};
  if (!location) return res.status(400).json({ error: 'location required' });

  try {
    const input = {
      searchStringsArray: ["dentist","dental clinic","cosmetic dentist","orthodontist"],
      locationQuery: location,
      maxCrawledPlacesPerSearch: Math.ceil(maxResults/4),
      includeWebsite: true,
      skipPlacesWithoutWebsite: true,
      additionalInfo: true,
      enrichPlaceWithBusinessLeads: true,
      minReviews: 10,
      minRating: 3.5
    };

    // For local dev, webhooks will fail (Apify can't reach localhost).
    // We'll deploy to Render and trigger /api/generate there.
    const run = await apify.actor(ACTORS.places).call(input, {
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
      meta: { location, maxResults }
    });

    res.json({ ok: true, runId: run.id });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/apify/webhook', async (req, res) => {
  res.status(200).json({ received: true }); // ack immediately

  try {
    const runId = req.body?.resource?.id || req.body?.id || req.body?.resource?.defaultDatasetId?.split('/').pop();
    if (!runId) return;

    const run = await apify.run(runId);
    const { items } = await apify.dataset(run.defaultDatasetId).listItems();

    const limit = pLimit(6);
    await Promise.all(items.map(item => limit(async () => {
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

      const id = await upsertLead(base);

      // MVP enrichment: fetch homepage text → detect features → LLM specialties
      let siteText = '';
      if (base.website) siteText = await fetchSiteText(base.website);

      const features = detectFeatures(siteText);
      const llm = await enrichWithLLM({ siteText });

      await saveTechMvp(id, features, siteText, llm);
      await rescore(id);

      await supabase.from('lead_events').insert({ lead_id: id, event_type: 'created', payload: { runId } });
    })));

    await supabase.from('lead_runs').update({ status: 'succeeded', finished_at: new Date() }).eq('run_id', runId);
  } catch (e) {
    console.error('webhook', e);
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`API listening on :${PORT}`));
