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

// 🔊 Request logger so we see every hit in Render logs
app.use((req, _res, next) => {
  try { console.log('REQ', req.method, req.url); } catch (_) {}
  next();
});

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const apify = new ApifyClient({ token: process.env.APIFY_API_TOKEN });

const ACTORS = {
  places: 'compass/crawler-google-places',
  rag: process.env.APIFY_RAG_ACTOR || 'apify/rag-web-browser',
  deep: process.env.APIFY_DEEP_CONTACTS_ACTOR || 'peterasorensen/snacci'
};

async function runActorAndGetItems(actorSlug, input) {
  const run = await apify.actor(actorSlug).call(input);
  const { items } = await apify.dataset(run.defaultDatasetId).listItems({ limit: 1000 });
  return { run, items };
}

/** Simple concurrency helper (no external deps) */
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

/** Retry helper for transient fetch errors */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function withRetry(fn, { tries = 3, baseMs = 400 } = {}) {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try { return await fn(); } catch (e) {
      lastErr = e;
      const msg = String(e?.message || e);
      if (!/fetch failed|ECONN|ENOTFOUND|ETIMEDOUT|socket hang up/i.test(msg)) break;
      await sleep(baseMs * Math.pow(2, i));
    }
  }
  throw lastErr;
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

/** ---- Supabase helpers (with retries) ---- */
async function sbSelectOr(table, orCond) {
  return withRetry(async () => {
    const q = await supabase.from(table).select('id').or(orCond).limit(1);
    if (q.error) throw q.error;
    return q.data;
  });
}
async function sbInsertOne(table, payload) {
  return withRetry(async () => {
    const { data, error } = await supabase.from(table).insert(payload).select('id').single();
    if (error) throw error;
    return data;
  });
}
async function sbUpdate(table, payload, byId) {
  return withRetry(async () => {
    const { error } = await supabase.from(table).update(payload).eq('id', byId);
    if (error) throw error;
  });
}
async function sbInsert(table, payload) {
  return withRetry(async () => {
    const { error } = await supabase.from(table).insert(payload);
    if (error) throw error;
  });
}
async function sbSelectOne(table, cols, match) {
  return withRetry(async () => {
    const { data, error } = await supabase.from(table).select(cols).match(match).single();
    if (error && error.code !== 'PGRST116') throw error; // no rows found is fine
    return data || null;
  });
}

async function upsertLead(base) {
  const domain = domainFrom(base.website);
  const conds = [
    base.google_place_id ? `google_place_id.eq.${base.google_place_id}` : null,
    domain ? `domain.eq.${domain}` : null,
    base.phone ? `phone.eq.${base.phone}` : null
  ].filter(Boolean).join(',');

  let existing = null;
  if (conds) {
    const found = await sbSelectOr('dental_leads', conds);
    existing = found?.[0] || null;
  }

  const payload = {
    google_place_id: base.google_place_id || null,
    name: base.name,
    address: base.address,
    // domain: generated by DB; do not write
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
    await sbUpdate('dental_leads', payload, existing.id);
    return existing.id;
  } else {
    const d = await sbInsertOne('dental_leads', payload);
    return d.id;
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
  const existing = await sbSelectOne('lead_tech_analysis', 'id', { lead_id });
  if (existing) await sbUpdate('lead_tech_analysis', row, existing.id);
  else await sbInsert('lead_tech_analysis', row);
}

async function rescore(lead_id) {
  const lead = await sbSelectOne('dental_leads', '*', { id: lead_id });
  const tech = await sbSelectOne('lead_tech_analysis', '*', { lead_id });

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

  await withRetry(async () => {
    const { error } = await supabase.from('dental_leads').update({
      tech_score: techScore,
      tech_tier: tier,
      investment_level: tier,
      qualification_status: qual,
      final_score: ss.final,
      final_score_explanation: tech?.llm_notes || null
    }).eq('id', lead_id);
    if (error) throw error;
  });

  await sbInsert('lead_events', { lead_id, event_type: 'rescored', payload: { ss, tier, qual } });
}

/** ============= Routes ============= **/
app.get('/api/health', (_, res) => res.json({ ok: true }));

// Deep health: check env & Supabase connectivity (no secrets shown)
app.get('/api/debug/connections', async (_, res) => {
  const env = {
    publicBaseUrl: process.env.PUBLIC_BASE_URL || '',
    supabaseUrlLooksOk: !!process.env.SUPABASE_URL && /\.supabase\.co$/.test(new URL(process.env.SUPABASE_URL).hostname),
    supabaseKeyLen: (process.env.SUPABASE_SERVICE_KEY || '').length,
    apifyKeyLen: (process.env.APIFY_API_TOKEN || '').length,
    anthropicKeyLen: (process.env.ANTHROPIC_API_KEY || '').length
  };

  let supabasePing = { ok: false, error: null, count: null };
  try {
    const { error, count } = await supabase
      .from('dental_leads')
      .select('id', { count: 'exact', head: true });
    if (error) supabasePing.error = { message: error.message, code: error.code || null };
    else supabasePing.ok = true, supabasePing.count = count ?? 0;
  } catch (e) {
    supabasePing.error = { message: String(e?.message || e) };
  }

  res.json({ ok: true, env, supabasePing });
});

// Generate: use actor().start so it returns immediately
app.post('/api/generate', async (req, res) => {
  const {
    location,
    maxResults = 40,
    preset = 'general',
    minRating = 3.8,
    minReviews = 10,
    avoidChains = true,
    includeKeywords = [],
    excludeKeywords = []
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

    await sbInsert('lead_runs', {
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

// Temporary GET handler for quick check
app.get('/api/apify/webhook', (_req, res) => {
  console.log('WEBHOOK GET HIT');
  res.json({ ok: true, method: 'GET' });
});

// Robust webhook: resolve datasetId from multiple places; bail gracefully if missing
app.post('/api/apify/webhook', async (req, res) => {
  res.status(200).json({ received: true }); // ack immediately
  console.log('WEBHOOK POST HIT');

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
      const run = await apify.run(runId).get();
      finalDatasetId = run?.defaultDatasetId || null;
    }
    if (!finalDatasetId) {
      console.warn('webhook: unable to resolve datasetId');
      return;
    }
    console.log('WEBHOOK RESOLVED', { runId, finalDatasetId });

    const { items } = await apify.dataset(finalDatasetId).listItems({ limit: 1000 });
    console.log('WEBHOOK ITEMS', items.length);

    const runMetaRow = await sbSelectOne('lead_runs','meta',{ run_id: runId });
    const opts = runMetaRow?.meta || {};
    const avoidChains = opts.avoidChains ?? true;
    const includeKeywords = Array.isArray(opts.includeKeywords) ? opts.includeKeywords : [];
    const excludeKeywords = Array.isArray(opts.excludeKeywords) ? opts.excludeKeywords : [];

    await mapLimit(items, 3, async (item) => {
      console.log('PROCESSING', item?.title, item?.website || item?.url || '');

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

      if (avoidChains && DEFAULT_CHAINS.some(c => (base.name || '').toLowerCase().includes(c))) return;

      const id = await upsertLead(base);

      const logPrefix = `[lead] ${base.google_place_id || base.name || ''}`;
      const t0 = Date.now();

      // (1) Prefer RAG actor for website text; fallback to local fetch
      let siteText = '';
      if (base.website) {
        try {
          console.log(logPrefix, 'RAG start →', ACTORS.rag);
          const ragInput = {
            query: base.website,                // single URL mode
            outputFormats: ['markdown'],
            scrapingTool: 'raw-http',
            requestTimeoutSecs: 40
          };
          const { items: ragItems } = await runActorAndGetItems(ACTORS.rag, ragInput);
          const first = ragItems?.find(i => i?.markdown) || ragItems?.[0] || {};
          siteText = String(first.markdown || '').slice(0, 15000);
          if (!siteText) throw new Error('RAG returned no markdown');
          console.log(logPrefix, 'RAG done. chars=', siteText.length);
        } catch (e) {
          console.warn(logPrefix, 'RAG failed → fallback fetchSiteText:', e?.message || e);
          siteText = await fetchSiteText(base.website);
        }
      }

      // (2) Keyword filters
      const low = (siteText || '').toLowerCase();
      if (includeKeywords.length && !includeKeywords.some(k => low.includes(String(k).toLowerCase()))) return;
      if (excludeKeywords.length && excludeKeywords.some(k => low.includes(String(k).toLowerCase()))) return;

      // (3) Deep contacts via snacci
      let deepContacts = [];
      if (base.website) {
        try {
          console.log(logPrefix, 'Deep contacts start →', ACTORS.deep);
          const deepInput = {
            websites: [base.website],
            scrapeTypes: ['emails','phoneNumbers','socialMedia'],
            removeDuplicates: true,
            maxDepth: 2,
            maxLinksPerPage: 100
          };
          const { items } = await runActorAndGetItems(ACTORS.deep, deepInput);
          deepContacts = Array.isArray(items) ? items : [];
          await sbInsert('lead_events', {
            lead_id: id,
            event_type: 'contacts_found',
            payload: { count: deepContacts.length }
          });
        } catch (e) {
          console.warn(logPrefix, 'Deep contacts failed:', e?.message || e);
        }
      }

      const features = detectFeatures(siteText);
      const llm = await enrichWithLLM({ siteText });

      await saveTechMvp(id, features, siteText, llm);
      await rescore(id);

      await sbInsert('lead_events', { lead_id: id, event_type: 'created', payload: { runId, datasetId: finalDatasetId } });

      console.log(logPrefix, 'processed in', (Date.now() - t0) + 'ms');
    });

    await withRetry(async () => {
      const { error } = await supabase.from('lead_runs')
        .update({ status: 'succeeded', finished_at: new Date() })
        .eq('run_id', runId);
      if (error) throw error;
    });

  } catch (e) {
    console.error('webhook', e);
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`API listening on :${PORT}`));