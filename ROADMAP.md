# Crossref Research Intelligence — ROADMAP

> Roadmap untuk memanfaatkan **Crossref REST API** secara maksimal sebagai scholarly metadata backbone untuk research tooling, KMS/ARGUS, RAG, dan knowledge graph.

---

## 0. Vision

Build a reusable **Scholarly Metadata & Research Intelligence Service** powered by Crossref and enriched by other scholarly data sources.

### Target Architecture

```text
                    ┌──────────────────┐
                    │     Clients      │
                    │ Web / KMS / AI   │
                    └────────┬─────────┘
                             │
                             ▼
                 ┌───────────────────────┐
                 │ Research Intelligence │
                 │        API            │
                 └───────────┬───────────┘
                             │
             ┌───────────────┼────────────────┐
             │               │                │
             ▼               ▼                ▼
        ┌─────────┐     ┌──────────┐    ┌────────────┐
        │ Crossref│     │ OpenAlex │    │ Semantic   │
        │         │     │          │    │ Scholar    │
        └────┬────┘     └────┬─────┘    └─────┬──────┘
             │               │                │
             └───────────────┼────────────────┘
                             ▼
                    ┌────────────────┐
                    │ Normalization  │
                    │ & Deduplication│
                    └───────┬────────┘
                            │
               ┌────────────┼────────────┐
               ▼            ▼            ▼
          PostgreSQL      Qdrant       Object
          Metadata        Vectors      Storage
               │            │
               └──────┬─────┘
                      ▼
               Research Graph
                      │
                      ▼
               AI / RAG / Agent
```

---

# Phase 1 — Crossref Foundation

## Goal

Build a reliable Crossref client and understand the complete metadata model.

### Tasks

- [ ] Create Crossref API client
- [ ] Support `GET /works/{DOI}`
- [ ] Support `GET /works`
- [ ] Support query parameters
- [ ] Implement pagination
- [ ] Implement filtering
- [ ] Implement sorting
- [ ] Handle Crossref errors
- [ ] Add request timeout
- [ ] Add retry with exponential backoff
- [ ] Add rate-limit awareness
- [ ] Add structured logging
- [ ] Add API response caching

### Initial endpoints

```text
GET /works/{DOI}
GET /works
GET /journals
GET /members
GET /prefixes
GET /types
```

### Basic CLI

```bash
crossref doi 10.1016/j.eswa.2016.04.008

crossref search "multimodal AI"

crossref search --title "machine learning"

crossref search --author "Geoffrey Hinton"

crossref search --from-pub-date 2020-01-01
```

### Deliverable

```text
crossref-client/
├── client
├── models
├── cache
├── errors
├── cli
└── tests
```

---

# Phase 2 — DOI Intelligence

## Goal

Turn a DOI into a complete normalized research record.

### DOI pipeline

```text
DOI
 ↓
Normalize
 ↓
Validate
 ↓
Crossref lookup
 ↓
Normalize metadata
 ↓
Store
```

### DOI normalization

Support:

```text
10.1016/j.eswa.2016.04.008

https://doi.org/10.1016/j.eswa.2016.04.008

https://dx.doi.org/10.1016/j.eswa.2016.04.008

doi:10.1016/j.eswa.2016.04.008
```

All become:

```text
10.1016/j.eswa.2016.04.008
```

### Metadata model

Store:

```text
DOI
Title
Subtitle
Authors
Editors
Publisher
Journal
ISSN
ISBN
Type
Published date
Online published date
Volume
Issue
Pages
URL
Abstract
Subjects
References
License
Funding
ORCID
Affiliations
Update history
Relation
```

### Deliverable

Normalized schema:

```json
{
  "doi": "...",
  "title": "...",
  "authors": [],
  "publisher": "...",
  "journal": "...",
  "publication_date": "...",
  "volume": "...",
  "issue": "...",
  "pages": "...",
  "type": "...",
  "url": "...",
  "references": [],
  "subjects": [],
  "license": []
}
```

---

# Phase 3 — Citation & Bibliography Engine

## Goal

Make DOI → citation generation trivial.

### Support

- [ ] APA
- [ ] IEEE
- [ ] Vancouver
- [ ] Chicago
- [ ] MLA
- [ ] Harvard
- [ ] BibTeX
- [ ] RIS
- [ ] CSL JSON

### API

```http
GET /v1/citations/{doi}
```

```http
GET /v1/citations/{doi}?style=apa
```

### Example

```text
Input:
10.1016/j.eswa.2016.04.008

Output:
IEEE
APA
BibTeX
RIS
```

### Bulk mode

```http
POST /v1/citations/bulk
```

```json
{
  "dois": ["10.xxxx/aaa", "10.xxxx/bbb", "10.xxxx/ccc"],
  "style": "ieee"
}
```

---

# Phase 4 — Scholarly Search

## Goal

Use Crossref as a searchable research metadata index.

### Search capabilities

Support:

```text
title
author
ORCID
publisher
journal
DOI
ISSN
subject
type
institution
date
license
```

### Example

```http
GET /v1/search?q=multimodal+AI
```

Advanced:

```http
GET /v1/search
  ?title=multimodal+AI
  &from=2020-01-01
  &until=2026-12-31
  &type=journal-article
```

### Search pipeline

```text
User query
    ↓
Crossref search
    ↓
Metadata normalization
    ↓
Deduplication
    ↓
Filtering
    ↓
Ranking
    ↓
Results
```

---

# Phase 5 — Metadata Quality & Deduplication

## Goal

Prevent garbage metadata from polluting the research database.

### Deduplication keys

Priority:

```text
1. DOI
2. PMID
3. ISBN
4. normalized title + author + year
```

### Normalization

Normalize:

```text
Titles
Author names
DOIs
Dates
Journals
Publishers
ISSN
ORCID
```

### Quality scoring

Calculate:

```text
metadata_completeness
author_completeness
identifier_completeness
publication_completeness
```

Example:

```json
{
  "metadata_quality": 0.94,
  "missing": ["abstract"]
}
```

Do NOT treat this as a scientific quality score.

It is purely metadata quality.

---

# Phase 6 — Crossref Sync Engine

## Goal

Maintain a local scholarly metadata database instead of hitting Crossref for every request.

### Architecture

```text
Crossref
   │
   ▼
Sync Worker
   │
   ▼
Normalizer
   │
   ▼
PostgreSQL
```

### Features

- [ ] Initial bulk synchronization
- [ ] Incremental synchronization
- [ ] Retry queue
- [ ] Failed-record queue
- [ ] Change detection
- [ ] Metadata versioning
- [ ] Last-synced timestamp

### Database

Suggested tables:

```text
works
authors
work_authors
journals
publishers
identifiers
licenses
funders
affiliations
references
relations
work_updates
sync_jobs
```

---

# Phase 7 — Reference Graph

## Goal

Transform paper metadata into a research graph.

```text
Paper A
 ├── cites → Paper B
 ├── cites → Paper C
 └── cites → Paper D
```

### Graph entities

```text
Work
Author
Journal
Publisher
Institution
Funder
Concept
```

### Relationships

```text
AUTHORED_BY
PUBLISHED_IN
PUBLISHED_BY
CITES
CITED_BY
FUNDED_BY
AFFILIATED_WITH
RELATED_TO
```

### Example

```text
Author
   │
   ├── wrote → Paper A
   │             │
   │             ├── cites → Paper B
   │             └── cites → Paper C
   │
   └── wrote → Paper D
```

---

# Phase 8 — Multi-Source Scholarly Enrichment

Crossref should NOT be the only source.

Introduce an enrichment layer:

```text
                    ┌──────────┐
                    │ Crossref │
                    └────┬─────┘
                         │
┌──────────┐             │             ┌────────────────┐
│ OpenAlex │─────────────┼─────────────│ SemanticScholar│
└──────────┘             │             └────────────────┘
                         │
                  ┌──────▼──────┐
                  │  Resolver   │
                  └──────┬──────┘
                         │
                  Canonical Work
```

### Potential sources

- [ ] Crossref
- [ ] OpenAlex
- [ ] Semantic Scholar
- [ ] OpenAIRE
- [ ] PubMed / PMC
- [ ] arXiv
- [ ] Unpaywall
- [ ] CORE

### Source priority

Do not blindly overwrite metadata.

Instead:

```text
Crossref.title
OpenAlex.title
SemanticScholar.title
        ↓
Canonicalization
        ↓
Canonical title
```

Maintain provenance:

```json
{
  "value": "...",
  "source": "crossref",
  "retrieved_at": "..."
}
```

---

# Phase 9 — Open Access & Full-Text Discovery

## Goal

Crossref identifies the work; other sources locate accessible content.

```text
DOI
 ↓
Crossref
 ↓
OA resolver
 ↓
Repository / Publisher
 ↓
PDF / HTML
```

### Store

```text
landing_page
pdf_url
html_url
repository
oa_status
license
```

### Important

Never assume:

```text
DOI exists == PDF is freely accessible
```

Treat metadata, access rights, and full text as separate concepts.

---

# Phase 10 — PDF → Research Knowledge

Now integrate the research ingestion pipeline.

```text
PDF
 ↓
Document Parser
 ↓
Sections
 ↓
Paragraphs
 ↓
Tables
 ↓
Figures
 ↓
References
 ↓
Chunks
 ↓
Embeddings
 ↓
Qdrant
```

Crossref provides the canonical metadata layer:

```text
document
 ├── DOI
 ├── title
 ├── authors
 ├── journal
 ├── year
 ├── metadata
 └── full_text
```

---

# Phase 11 — Research RAG

## Goal

Build an AI assistant that understands a user's paper collection.

Example:

```text
User:
"What are the main approaches for multimodal
reranking after 2022?"
```

Pipeline:

```text
Question
   ↓
Query expansion
   ↓
Metadata filtering
   ↓
Qdrant retrieval
   ↓
Crossref metadata
   ↓
Reranking
   ↓
LLM synthesis
   ↓
Cited answer
```

### Retrieval dimensions

Combine:

```text
semantic similarity
+
keyword search
+
metadata filters
+
author
+
year
+
journal
+
citation graph
```

---

# Phase 12 — Research Discovery Engine

## Goal

Move from "search papers" to "discover research".

### Features

- [ ] Related works
- [ ] Similar papers
- [ ] Author discovery
- [ ] Topic discovery
- [ ] Citation neighborhood
- [ ] Research timeline
- [ ] Emerging topic detection
- [ ] Literature gap exploration

Example:

```text
Topic:
Multimodal RAG
      │
      ├── 2020
      ├── 2021
      ├── 2022
      ├── 2023
      ├── 2024
      ├── 2025
      └── 2026
```

---

# Phase 13 — Research Timeline

Generate:

```text
Topic → Papers → Years → Methods
```

Example:

```text
2019 ── Method A
2020 ── Method B
2021 ── Method C
2022 ── Method D
2023 ── Method E
2024 ── Method F
2025 ── Method G
```

Useful for:

- Literature review
- Thesis background
- Research proposal
- Systematic literature review
- Research trend analysis

---

# Phase 14 — Author Intelligence

Build author-level metadata.

```text
Author
 ├── publications
 ├── affiliations
 ├── ORCID
 ├── topics
 ├── coauthors
 └── citation relationships
```

### Co-author graph

```text
Alice ─── Bob
  │        │
  │        └── Charlie
  │
  └── David
```

Useful for:

- finding research groups
- discovering collaborators
- mapping research communities

Do not interpret graph centrality as scientific quality.

---

# Phase 15 — Journal & Publisher Intelligence

Build:

```text
Journal
 ├── publisher
 ├── ISSN
 ├── works
 ├── subjects
 ├── publication timeline
 └── metadata statistics
```

Useful for:

```text
"What journals publish research about X?"
```

Keep this descriptive rather than treating metadata as a journal-quality ranking.

---

# Phase 16 — Research Knowledge Graph

Combine everything:

```text
                 ┌──────── Author
                 │
                 ▼
Journal ←── Paper ──→ Topic
                 │
                 ├── cites → Paper
                 ├── funded by → Funder
                 ├── related → Paper
                 └── published by → Publisher
```

Storage:

```text
PostgreSQL
    +
Qdrant
    +
Object Storage
```

Postgres:

```text
structured relationships
```

Qdrant:

```text
semantic relationships
```

Object Storage:

```text
PDF / HTML / extracted assets
```

---

# Phase 17 — ARGUS Integration

Crossref becomes one of ARGUS's ingestion sources.

```text
                    ARGUS
                      │
        ┌─────────────┼──────────────┐
        │             │              │
        ▼             ▼              ▼
   Documents      Research       Enterprise
        │             │              │
        │             ▼              │
        │          Crossref           │
        │             │              │
        └─────────────┼──────────────┘
                      ▼
                 Knowledge Base
```

### ARGUS capabilities

```text
Upload paper
      ↓
Detect DOI
      ↓
Crossref enrichment
      ↓
Fetch metadata
      ↓
Download OA version
      ↓
Parse PDF
      ↓
Chunk
      ↓
Embed
      ↓
Index
```

Result:

```text
One-click paper ingestion
```

---

# Phase 18 — Omniflow Research Service

Expose it as a reusable Omniflow microservice.

```text
research.omniflow.id
```

### API

```http
GET  /v1/works/{doi}
GET  /v1/search
GET  /v1/authors/{id}
GET  /v1/journals/{issn}

POST /v1/works/resolve
POST /v1/works/bulk
POST /v1/citations
POST /v1/enrich
POST /v1/ingest
```

### Internal services

```text
research-api
research-worker
metadata-normalizer
citation-service
enrichment-worker
embedding-worker
graph-worker
```

---

# Phase 19 — MCP Research Agent

Expose the research system to AI agents.

Possible tools:

```text
search_papers
resolve_doi
get_paper
get_author
get_journal
get_references
find_related_papers
find_open_access_version
generate_citation
compare_papers
build_literature_map
```

Example agent workflow:

```text
User:
"Find papers about adaptive reranking from 2022-2026."

Agent
 ↓
search_papers()
 ↓
metadata filtering
 ↓
semantic reranking
 ↓
find_related_papers()
 ↓
literature_map()
 ↓
answer with citations
```

---

# Phase 20 — Automated Literature Review

Eventually support:

```text
Research question
      ↓
Query generation
      ↓
Crossref discovery
      ↓
Multi-source enrichment
      ↓
Deduplication
      ↓
Eligibility filtering
      ↓
Full-text acquisition
      ↓
PDF parsing
      ↓
Evidence extraction
      ↓
Synthesis
      ↓
Bibliography
```

Generate structured output:

```text
Research Question
Scope
Search Strategy
Included Studies
Excluded Studies
Methods
Datasets
Findings
Limitations
Research Gaps
References
```

Important:

AI-generated conclusions must remain traceable to source papers.

---

# Phase 21 — Observability

Instrument everything.

Metrics:

```text
crossref_requests_total
crossref_errors_total
crossref_latency
cache_hit_ratio

works_ingested_total
works_enriched_total
works_failed_total

metadata_completeness
deduplication_rate

pdf_discovery_success_rate
embedding_success_rate
```

Dashboard:

```text
Request rate
Error rate
Latency
Cache hit ratio
Ingestion throughput
Enrichment coverage
```

---

# Phase 22 — Production Hardening

### Reliability

- [ ] Retry policy
- [ ] Circuit breaker
- [ ] Timeout
- [ ] Queue
- [ ] Dead-letter queue
- [ ] Idempotent ingestion
- [ ] Backpressure

### Data

- [ ] Schema migrations
- [ ] Provenance tracking
- [ ] Metadata versioning
- [ ] Soft deletion
- [ ] Audit log

### Security

- [ ] API authentication
- [ ] API keys
- [ ] Rate limiting
- [ ] Tenant isolation
- [ ] Usage accounting

### Cost

Prefer:

```text
Cache
 ↓
Local database
 ↓
Only query external APIs when necessary
```

---

# Phase 23 — Research API as an Omniflow Product

Potential product:

## Omniflow Research API

```text
DOI Resolution
Metadata
Citation
Search
Enrichment
Open Access Discovery
Research Graph
Semantic Search
RAG
MCP
```

Potential users:

```text
Universities
Research Labs
Publishers
Libraries
Students
Researchers
Enterprise R&D
AI Research Agents
```

---

# Recommended MVP Sequence

Do NOT build everything at once.

## MVP 1

```text
Crossref Client
    ↓
DOI Resolver
    ↓
Metadata Normalizer
    ↓
PostgreSQL
```

Deliver:

```text
DOI → canonical metadata
```

---

## MVP 2

```text
Search
+
Citation Generator
+
Bulk DOI Resolution
```

Deliver:

```text
Research metadata API
```

---

## MVP 3

```text
Crossref
+
OpenAlex
+
Semantic Scholar
```

Deliver:

```text
Unified scholarly metadata
```

---

## MVP 4

```text
PDF
 ↓
Parser
 ↓
Qdrant
 ↓
Research RAG
```

Deliver:

```text
AI Research Assistant
```

---

## MVP 5

```text
Citation Graph
+
Author Graph
+
Topic Graph
```

Deliver:

```text
Research Knowledge Graph
```

---

## MVP 6

```text
ARGUS
+
Omniflow
+
MCP
```

Deliver:

```text
Research Intelligence Platform
```

---

# Suggested Repository

```text
crossref-research/
│
├── apps/
│   ├── api/
│   ├── worker/
│   └── cli/
│
├── packages/
│   ├── crossref/
│   ├── openalex/
│   ├── semantic-scholar/
│   ├── metadata/
│   ├── citation/
│   ├── graph/
│   └── embeddings/
│
├── migrations/
│
├── tests/
│
├── docs/
│
└── ROADMAP.md
```

---

# Suggested Initial Stack

```text
API
Bun + Elysia

Worker
Go

Database
PostgreSQL

Vector DB
Qdrant

Queue
RabbitMQ

Object Storage
S3-compatible

Search
PostgreSQL FTS + Qdrant

AI
OpenAI-compatible API

Observability
OpenTelemetry + LGTM
```

This intentionally fits the existing Omniflow architecture.

---

# Core Design Principles

### 1. Crossref is metadata infrastructure

Do not treat Crossref as the entire research ecosystem.

```text
Crossref = scholarly identity + metadata
```

---

### 2. Preserve provenance

Never silently overwrite:

```text
Crossref
OpenAlex
Semantic Scholar
Publisher
Repository
```

Store source + timestamp.

---

### 3. DOI is a first-class identifier

Use DOI as the primary canonical identifier whenever available.

---

### 4. Separate metadata from content

```text
Metadata
≠
Full text
≠
Embedding
≠
Citation graph
```

Keep these layers independent.

---

### 5. Cache aggressively

External API:

```text
slow / limited / unreliable
```

Local database:

```text
fast / deterministic / queryable
```

---

### 6. Make every enrichment reproducible

Every derived field should be traceable to:

```text
source
retrieved_at
processor_version
```

---

# Final Target

The end state is not:

> "an application that calls Crossref."

It is:

> **A reusable scholarly intelligence layer where Crossref provides canonical publication metadata, while OpenAlex/Semantic Scholar provide enrichment, Qdrant provides semantic retrieval, PostgreSQL provides structured knowledge, and AI agents provide research workflows.**

```text
                     OMNIFLOW
                         │
                    ┌────▼────┐
                    │ ARGUS   │
                    └────┬────┘
                         │
              Research Intelligence
                         │
        ┌────────────────┼────────────────┐
        │                │                │
    Crossref          OpenAlex       Semantic Scholar
        │                │                │
        └────────────────┼────────────────┘
                         │
                  Canonical Works
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
         PostgreSQL    Qdrant     Object Storage
             │           │           │
             └───────────┼───────────┘
                         ▼
                 Knowledge Graph
                         │
                         ▼
                    RAG / MCP
                         │
                         ▼
                Research Agents
                         │
                         ▼
             Literature Intelligence
```

**North Star:**
`DOI → Metadata → Knowledge → Evidence → Intelligence`
