const Anthropic = require('@anthropic-ai/sdk');
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// Extract specialties + 1–2 sentence rationale from site text
async function enrichWithLLM({ siteText }) {
  const prompt = `You analyze dental practice websites. Return STRICT JSON with keys {specialties, notes}.
- specialties: array from {cosmetic, aligners, implants, sedation, pediatric, ortho, perio, prostho, endo}
- notes: one 1–2 sentence rationale.
If unsure, return empty array and empty notes. Do not include any extra keys.
Text:
${(siteText || '').slice(0, 2000)}`;

  const resp = await anthropic.messages.create({
    model: 'claude-3-5-haiku-latest',
    max_tokens: 300,
    temperature: 0.2,
    messages: [{ role: 'user', content: prompt }]
  });

  const text = resp?.content?.[0]?.text || '';
  try {
    const data = JSON.parse(text);
    const specialties = Array.isArray(data.specialties) ? data.specialties : [];
    const notes = typeof data.notes === 'string' ? data.notes.slice(0, 300) : '';
    return { specialties, notes };
  } catch {
    return { specialties: [], notes: '' };
  }
}

module.exports = { enrichWithLLM };
