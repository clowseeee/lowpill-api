// /api/read.js — retourne un payload “produit” pour Willo + Front
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

module.exports = async (req, res) => {
  try {
    const { company, metric, limit = 8 } = req.query || {};
    if (!company) {
      return res.status(400).json({ error: 'Missing ?company=<slug or name>' });
    }

    // 1) Résoudre le slug → id/slug
    const { data: comp, error: cErr } = await supabase
      .from('companies')
      .select('id, slug, name, domain')
      .or(`slug.eq.${company},name.ilike.%${company}%`)
      .limit(1)
      .maybeSingle();
    if (cErr) return res.status(500).json({ error: cErr.message });
    if (!comp) return res.status(404).json({ error: 'Company not found' });

    // 2) Facts analytiques (plus)
    let q = supabase
      .from('fact_analysis_plus')
      .select('*')
      .eq('company_slug', comp.slug)
      .order('as_of_date', { ascending: false })
      .limit(limit);

    if (metric) q = q.eq('metric_key', metric);

    const { data: facts, error: fErr } = await q;
    if (fErr) return res.status(500).json({ error: fErr.message });

    // 3) Narratifs
    let nq = supabase
      .from('fact_narratives')
      .select('*')
      .eq('company_slug', comp.slug)
      .order('as_of_date', { ascending: false })
      .limit(limit);

    if (metric) nq = nq.eq('metric_key', metric);

    const { data: narr, error: nErr } = await nq;
    if (nErr) return res.status(500).json({ error: nErr.message });

    // 4) Regroupement produit
    const metrics = {};
    for (const row of facts || []) {
      if (!metrics[row.metric_key]) metrics[row.metric_key] = { series: [], last: null };
      metrics[row.metric_key].series.push({
        date: row.as_of_date,
        value: row.metric_value_num,
        yoy: row.yoy_change,
        qoq: row.qoq_change,
        trend: row.trend,
        signal: row.signal_strength,
        zscore_sector: row.zscore_sector
      });
    }
    // Dernier point par metric
    for (const k of Object.keys(metrics)) {
      metrics[k].series.sort((a,b) => new Date(b.date) - new Date(a.date));
      metrics[k].last = metrics[k].series[0];
    }

    // 5) Narratifs simplifiés
    const narratives = (narr || []).map(n => ({
      date: n.as_of_date,
      metric: n.metric_key,
      fr: n.narrative_fr,
      en: n.narrative_en,
      yoy: n.yoy_change,
      qoq: n.qoq_change,
      trend: n.trend,
      signal: n.signal_strength
    }));

    return res.status(200).json({
      company: { slug: comp.slug, name: comp.name, domain: comp.domain },
      metrics,
      narratives
    });
  } catch (err) {
    console.error('READ ERROR:', err);
    return res.status(500).json({ error: err?.message || 'unknown' });
  }
};
// /api/read.js — Lowpill v1.6 (read + provenance-aware)
const { createClient } = require('@supabase/supabase-js');
const { z } = require('zod');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

// -------- utils ----------
const toSlug = (s) => (s || '').trim().toLowerCase();
const isFiniteNum = (x) => Number.isFinite(x) && !Number.isNaN(x);

// -------- validation ----------
const querySchema = z.object({
  company: z.string().min(1),
  metric: z.string().optional(),
  theme: z.string().optional(),
  limit: z.coerce.number().int().min(1).max(50).optional()
});

// -------- compute helpers ----------
function computeChanges(series) {
  // series: [{date, value}] sorted asc by date
  for (let i = 0; i < series.length; i++) {
    const cur = series[i];
    const prev = i > 0 ? series[i - 1] : null;
    cur.qoq = prev && isFiniteNum(prev.value) && prev.value !== 0
      ? Number(((100 * (cur.value - prev.value)) / prev.value).toFixed(2))
      : null;

    // YOY naïf = même index -1 si périodicité inconnue; si tu veux YOY vrai, passe via ta vue SQL
    cur.yoy = cur.qoq; // fallback simple si pas de périodicité
    cur.trend = isFiniteNum(cur.qoq) ? (cur.qoq > 0 ? 'up' : (cur.qoq < 0 ? 'down' : 'flat')) : 'flat';
  }
  return series;
}

function scoreToSignal(s) {
  if (s == null) return 'none';
  if (s >= 0.85) return 'strong';
  if (s >= 0.65) return 'moderate';
  if (s >= 0.50) return 'weak';
  return 'none';
}

// -------- handler ----------
module.exports = async (req, res) => {
  try {
    if (req.method !== 'GET') {
      return res.status(405).json({ error: 'Method not allowed' });
    }

    // 1) Parse & validate query
    const parsed = querySchema.parse({
      company: req.query.company,
      metric: req.query.metric,
      theme: req.query.theme,
      limit: req.query.limit
    });

    const slug = toSlug(parsed.company);

    // 2) Company
    const { data: company, error: cErr } = await supabase
      .from('companies')
      .select('id, slug, name, domain')
      .eq('slug', slug)
      .maybeSingle();
    if (cErr) return res.status(500).json({ error: `company select: ${cErr.message}` });
    if (!company) return res.status(404).json({ error: 'company not found' });

    // 3) Metrics (optionnel si ?metric=)
    let metrics = {};
    if (parsed.metric) {
      const { data: factRows, error: fErr } = await supabase
        .from('facts')
        .select('as_of_date, metric_value_num')
        .eq('company_id', company.id)
        .eq('metric_key', parsed.metric)
        .order('as_of_date', { ascending: true });
      if (fErr) return res.status(500).json({ error: `facts select: ${fErr.message}` });

      const series = (factRows || [])
        .filter(r => r.metric_value_num != null)
        .map(r => ({ date: r.as_of_date, value: Number(r.metric_value_num) }));

      computeChanges(series);

      const last = series.length ? series[series.length - 1] : null;

      // Z-score sector (si dispo dans ta vue matérielle, sinon null)
      // Ici on laisse null par défaut — tu peux lier ta vue fact_analysis_sector pour enrichir.
      const zscore_sector = null;

      metrics[parsed.metric] = {
        series: series.map(s => ({
          date: s.date,
          value: s.value,
          yoy: s.yoy,
          qoq: s.qoq,
          trend: s.trend,
          signal: scoreToSignal(Math.abs((s.qoq ?? 0) / 100)), // signal simple basé sur variation
          zscore_sector
        })),
        last: last ? {
          date: last.date,
          value: last.value,
          yoy: last.yoy,
          qoq: last.qoq,
          trend: last.trend,
          signal: scoreToSignal(Math.abs((last.qoq ?? 0) / 100)),
          zscore_sector
        } : null
      };
    }

    // 4) Insights + provenance (tri par provenance_score)
    const lim = parsed.limit ?? 5;

    // filtre par thème éventuel
    const themeFilter = parsed.theme ? { theme_enum: toSlug(parsed.theme) } : {};

    // On calcule le score dans SQL pour être robuste même si NULLs
    const { data: insights, error: iErr } = await supabase
      .from('insights')
      .select(`
        id, company_id, source_id, theme_enum, theme, text, confidence, created_at,
        sources:source_id (
          id, url, title, published_at,
          publisher_domain, publisher_name, publisher_type, is_official, trust_score, source_type
        )
      `)
      .eq('company_id', company.id)
      .match(themeFilter)
      .order('created_at', { ascending: false }); // on récupère large puis on filtre/score côté app

    if (iErr) return res.status(500).json({ error: `insights select: ${iErr.message}` });

    const enriched = (insights || []).map(r => {
      const trust = r?.sources?.trust_score ?? 0;
      const conf = r?.confidence ?? 0;
      const provenance_score = Number((conf * trust).toFixed(3));
      return {
        id: r.id,
        date: r.created_at,
        theme: r.theme ?? r.theme_enum ?? 'other',
        text: r.text,
        confidence: conf,
        provenance_score,
        publisher: {
          name: r?.sources?.publisher_name ?? null,
          domain: r?.sources?.publisher_domain ?? null,
          type: r?.sources?.publisher_type ?? 'other',
          is_official: !!r?.sources?.is_official,
          trust_score: r?.sources?.trust_score ?? 0,
          doc_type: r?.sources?.source_type ?? 'other',
          url: r?.sources?.url ?? null,
          title: r?.sources?.title ?? null,
          published_at: r?.sources?.published_at ?? null
        }
      };
    });

    // Tri desc par provenance_score puis date
    enriched.sort((a, b) => (b.provenance_score - a.provenance_score) || (new Date(b.date) - new Date(a.date)));

    const topInsights = enriched.slice(0, lim);

    // 5) Agrégats utiles pour le front
    const avgProv = topInsights.length
      ? Number((topInsights.reduce((s, x) => s + (x.provenance_score ?? 0), 0) / topInsights.length).toFixed(3))
      : null;

    const byType = topInsights.reduce((acc, x) => {
      const k = x.publisher?.type || 'other';
      acc[k] = (acc[k] || 0) + 1;
      return acc;
    }, {});

    // 6) Narratives simples (si metric présent)
    const narratives = [];
    if (parsed.metric && metrics[parsed.metric]?.series?.length) {
      for (const s of metrics[parsed.metric].series) {
        narratives.push({
          date: s.date,
          metric: parsed.metric,
          fr: s.yoy != null
            ? `${company.name} affiche ${s.yoy > 0 ? 'une hausse' : 'une baisse'} de ${Math.abs(s.yoy).toFixed(2)}% de son ${parsed.metric} en ${new Date(s.date).getFullYear()}`
            : `${company.name} n'a pas d'historique comparable pour ${parsed.metric} en ${new Date(s.date).getFullYear()}`,
          en: s.yoy != null
            ? `${company.name}’s ${parsed.metric} ${s.yoy > 0 ? 'rose' : 'fell'} ${Math.abs(s.yoy).toFixed(2)}% YoY`
            : `${company.name} has no prior period to compare for ${parsed.metric} in ${new Date(s.date).getFullYear()}`,
          yoy: s.yoy,
          qoq: s.qoq,
          trend: s.trend,
          signal: s.signal
        });
      }
    }

    // 7) Réponse
    return res.status(200).json({
      company: { slug: company.slug, name: company.name, domain: company.domain ?? null },
      metrics, // vide si pas de ?metric=
      insights: {
        top: topInsights,
        aggregates: {
          avg_provenance_score: avgProv,
          count_by_publisher_type: byType
        }
      },
      narratives
    });

  } catch (err) {
    console.error('READ ERROR:', err);
    return res.status(500).json({ error: err?.message || 'unknown' });
  }
};
