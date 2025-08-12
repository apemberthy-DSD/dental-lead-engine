const Anthropic = require('@anthropic-ai/sdk');
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

async function runModel(model, siteText) {
  const prompt = `Extract as JSON:
- specialties: array from {cosmetic, aligners, implants, sedation, ortho, perio, prostho, endo}
- notes: one 1–2 sentence rationale.
Return ONLY JSON. Text:\n${(siteText || '').slice(0, 2000)}`;
  const resp = await anthropic.messages.create({
    model,
    max_tokens: 350,
    temperature: 0.2,
    messages: [{ role: 'user', content: prompt }],
  });
  const text = resp?.content?.[0]?.text || '';
  try {
    const data = JSON.parse(text);
    return {
      specialties: Array.isArray(data.specialties) ? data.specialties : [],
      notes: typeof data.notes === 'string' ? data.notes.slice(0, 300) : '',
    };
  } catch {
    return { specialties: [], notes: '' };
  }
}

async function enrichWithLLM({ siteText }) {
  // 1) Haiku (fast/cheap)
  let out = await runModel('claude-3-5-haiku-latest', siteText);

  // 2) Escalate to Sonnet only if Haiku is empty/weak AND we have enough text
  const tooWeak = !out.specialties?.length && (siteText || '').length > 800;
  if (tooWeak) {
    const sonnet = await runModel('claude-3-5-sonnet-latest', siteText);
    if (sonnet.specialties?.length) out = { ...sonnet, escalated: true };
  }

  return out;
}

module.exports = { enrichWithLLM };