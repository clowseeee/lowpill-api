import { createClient } from "@supabase/supabase-js";
import { z } from "zod";

const schema = z.object({
  company: z.string(),
  source: z.object({
    url: z.string(),
    title: z.string(),
    doc_type: z.string().optional(),
    date: z.string().optional(),
  }),
  facts: z
    .array(
      z.object({
        as_of_date: z.string().optional(),
        domain: z.string().optional(),
        metric_key: z.string(),
        metric_value: z.string(),
        unit: z.string().optional(),
        qualifier: z.string().optional(),
        source_quote: z.string().optional(),
        extraction_confidence: z.number().optional(),
      })
    )
    .optional(),
  events: z
    .array(
      z.object({
        date: z.string().optional(),
        type: z.string().optional(),
        summary: z.string().optional(),
        impacted_metrics: z.string().optional(),
        source_quote: z.string().optional(),
        confidence: z.number().optional(),
      })
    )
    .optional(),
});

export default async function handler(req: Request) {
  try {
    if (req.method === "GET") return new Response("pong", { status: 200 });

    const body = await req.json();
    const parsed = schema.parse(body);

    const supabase = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE!
    );

    // company
    const { data: existingCompany } = await supabase
      .from("companies")
      .select("*")
      .eq("slug", parsed.company.toLowerCase())
      .maybeSingle();

    const comp =
      existingCompany ??
      (
        await supabase
          .from("companies")
          .insert({ slug: parsed.company.toLowerCase(), name: parsed.company })
          .select()
          .single()
      ).data;

    // source
    const { data: sd, error: sdErr } = await supabase
      .from("source_docs")
      .insert({
        company_id: comp.id,
        url: parsed.source.url,
        title: parsed.source.title,
        doc_type: parsed.source.doc_type,
        doc_date: parsed.source.date ? new Date(parsed.source.date) : null,
      })
      .select()
      .single();
    if (sdErr) throw new Error(sdErr.message);

    // facts
    if (parsed.facts?.length) {
      const rows = parsed.facts.map((f) => ({
        company_id: comp.id,
        source_id: sd.id,
        as_of_date: f.as_of_date ? new Date(f.as_of_date) : null,
        domain: f.domain,
        metric_key: f.metric_key,
        metric_value: f.metric_value,
        unit: f.unit,
        qualifier: f.qualifier,
        source_quote: f.source_quote,
        extraction_confidence: f.extraction_confidence,
      }));
      const { error } = await supabase.from("facts").insert(rows);
      if (error) throw new Error(error.message);
    }

    // events
    if (parsed.events?.length) {
      const rows = parsed.events.map((e) => ({
        company_id: comp.id,
        source_id: sd.id,
        event_date: e.date ? new Date(e.date) : null,
        type: e.type,
        summary: e.summary,
        impacted_metrics: e.impacted_metrics,
        source_quote: e.source_quote,
        confidence: e.confidence,
      }));
      const { error } = await supabase.from("events").insert(rows);
      if (error) throw new Error(error.message);
    }

    return new Response(
      JSON.stringify({ ok: true, company: comp.slug, source_id: sd.id }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  } catch (err: any) {
    console.error("INGEST ERROR:", err.message || err, err.stack);
    return new Response(
      JSON.stringify({
        error: err.message || "unknown",
        stack: err.stack || null,
      }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}
import { createClient } from "@supabase/supabase-js";
import { z } from "zod";

const schema = z.object({
  company: z.string(),
  source: z.object({
    url: z.string(),
    title: z.string(),
    doc_type: z.string().optional(),
    date: z.string().optional(),
  }),
  facts: z
    .array(
      z.object({
        as_of_date: z.string().optional(),
        domain: z.string().optional(),
        metric_key: z.string(),
        metric_value: z.string(),
        unit: z.string().optional(),
        qualifier: z.string().optional(),
        source_quote: z.string().optional(),
        extraction_confidence: z.number().optional(),
      })
    )
    .optional(),
  events: z
    .array(
      z.object({
        date: z.string().optional(),
        type: z.string().optional(),
        summary: z.string().optional(),
        impacted_metrics: z.string().optional(),
        source_quote: z.string().optional(),
        confidence: z.number().optional(),
      })
    )
    .optional(),
});

export default async function handler(req: Request) {
  try {
    if (req.method === "GET") return new Response("pong", { status: 200 });

    const body = await req.json();
    const parsed = schema.parse(body);

    const supabase = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE!
    );

    const { data: existingCompany } = await supabase
      .from("companies")
      .select("*")
      .eq("slug", parsed.company.toLowerCase())
      .maybeSingle();

    const comp =
      existingCompany ??
      (
        await supabase
          .from("companies")
          .insert({ slug: parsed.company.toLowerCase(), name: parsed.company })
          .select()
          .single()
      ).data;

    const { data: sd, error: sdErr } = await supabase
      .from("source_docs")
      .insert({
        company_id: comp.id,
        url: parsed.source.url,
        title: parsed.source.title,
        doc_type: parsed.source.doc_type,
        doc_date: parsed.source.date ? new Date(parsed.source.date) : null,
      })
      .select()
      .single();
    if (sdErr) throw new Error(sdErr.message);

    if (parsed.facts?.length) {
      const rows = parsed.facts.map((f) => ({
        company_id: comp.id,
        source_id: sd.id,
        as_of_date: f.as_of_date ? new Date(f.as_of_date) : null,
        domain: f.domain,
        metric_key: f.metric_key,
        metric_value: f.metric_value,
        unit: f.unit,
        qualifier: f.qualifier,
        source_quote: f.source_quote,
        extraction_confidence: f.extraction_confidence,
      }));
      const { error } = await supabase.from("facts").insert(rows);
      if (error) throw new Error(error.message);
    }

    if (parsed.events?.length) {
      const rows = parsed.events.map((e) => ({
        company_id: comp.id,
        source_id: sd.id,
        event_date: e.date ? new Date(e.date) : null,
        type: e.type,
        summary: e.summary,
        impacted_metrics: e.impacted_metrics,
        source_quote: e.source_quote,
        confidence: e.confidence,
      }));
      const { error } = await supabase.from("events").insert(rows);
      if (error) throw new Error(error.message);
    }

    return new Response(
      JSON.stringify({ ok: true, company: comp.slug, source_id: sd.id }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  } catch (err: any) {
    console.error("INGEST ERROR:", err.message || err, err.stack);
    return new Response(
      JSON.stringify({
        error: err.message || "unknown",
        stack: err.stack || null,
      }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}


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
