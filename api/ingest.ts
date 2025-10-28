import { z } from "zod";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE!
);

const Fact = z.object({
  company: z.string(),
  as_of_date: z.string().optional(),
  domain: z.string().optional(),
  metric_key: z.string(),
  metric_value: z.string(),
  unit: z.string().optional(),
  qualifier: z.string().optional(),
  source_quote: z.string().optional(),
  confidence: z.number().min(0).max(1).optional(),
});

const Event = z.object({
  company: z.string(),
  date: z.string().optional(),
  type: z.string(),
  summary: z.string().optional(),
  impacted_metrics: z.string().optional(),
  source_quote: z.string().optional(),
  confidence: z.number().min(0).max(1).optional(),
});

const Payload = z.object({
  company: z.string(),
  doc: z.object({
    url: z.string().url(),
    title: z.string().optional(),
    as_of_date: z.string().optional(),
    content: z.string(),
  }),
  facts: z.array(Fact).default([]),
  events: z.array(Event).default([]),
});

export default async function handler(req: Request) {
  if (req.method !== "POST")
    return new Response("Method not allowed", { status: 405 });

  const body = await req.json();
  const parse = Payload.safeParse(body);
  if (!parse.success)
    return new Response(JSON.stringify(parse.error.flatten()), { status: 400 });

  const { company, doc, facts, events } = parse.data;

  // upsert company
  const { data: comp, error: ec } = await supabase
    .from("companies")
    .upsert({ slug: company.toLowerCase(), name: company }, { onConflict: "slug" })
    .select()
    .single();
  if (ec) return new Response(ec.message, { status: 500 });

  // insert source_doc
  const { data: sd, error: es } = await supabase
    .from("source_docs")
    .insert({
      company_id: comp.id,
      url: doc.url,
      title: doc.title ?? null,
      as_of_date: doc.as_of_date ? new Date(doc.as_of_date) : null,
      content: doc.content,
    })
    .select()
    .single();
  if (es) return new Response(es.message, { status: 500 });

  // facts
  if (facts.length) {
    const rows = facts.map(f => ({
      company_id: comp.id,
      source_id: sd.id,
      as_of_date: f.as_of_date ? new Date(f.as_of_date) : null,
      domain: f.domain ?? null,
      metric_key: f.metric_key,
      metric_value: f.metric_value,
      unit: f.unit ?? null,
      qualifier: f.qualifier ?? null,
      source_quote: f.source_quote ?? null,
      confidence: f.confidence ?? null,
    }));
    const { error } = await supabase.from("facts").insert(rows);
    if (error) return new Response(error.message, { status: 500 });
  }

  // events
  if (events.length) {
    const rows = events.map(e => ({
      company_id: comp.id,
      source_id: sd.id,
      event_date: e.date ? new Date(e.date) : null,
      type: e.type,
      summary: e.summary ?? null,
      impacted_metrics: e.impacted_metrics ?? null,
      source_quote: e.source_quote ?? null,
      confidence: e.confidence ?? null,
    }));
    const { error } = await supabase.from("events").insert(rows);
    if (error) return new Response(error.message, { status: 500 });
  }

  return new Response(JSON.stringify({ ok: true, company: comp.slug, source_id: sd.id }), { status: 200 });
}
