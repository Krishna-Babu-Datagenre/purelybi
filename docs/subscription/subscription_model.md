# Purely BI Subscription Plan & Pricing Model

This document outlines the subscription structure for Purely BI, including a detailed analysis of our Azure infrastructure operational costs and the resulting pricing tiers designed to hit our target profit margins.

## 1. Azure Infrastructure Cost Analysis

To establish a sustainable and profitable pricing model, we analyzed the existing resources in the `rg-purelybi-sync-v2-dev-ci` and `rg-purelybi-dev-ci` resource groups. Our infrastructure consists of:

### 1.1 Backend Compute (Azure App Service)
* **Resources:** `app-purelybi-backend-dev-ci`, `asp-purelybi-dev-ci`
* **Cost Driver:** The backend handles API requests and pulls user data directly into RAM for DuckDB queries.
* **Estimate:** The currently provisioned **B1 Basic Plan** costs a fixed **~$13.14 / month**, but lacks automatic scaling. For production, automatic scaling is required to handle memory spikes. A **P1 v3 Plan** (Linux) costs **~$119.72 / month** per instance.
* **Impact:** Assuming one P1 v3 instance can comfortably handle ~50 highly active concurrent users, the amortized compute cost is **~$2.40 / month per active user**.

### 1.2 Frontend Hosting (Azure Static Web Apps)
* **Resources:** `app-purelybi-frontend-dev-ci`
* **Cost Driver:** Bandwidth and static asset serving.
* **Impact:** Very low cost. Global edge caching means this adds a negligible **<$0.05 / month per user**.

### 1.3 Data Sync Compute (Azure Container Apps Jobs)
* **Resources:** `caj-purelybi-connector-v2-dev-ci`, `metadata-generator-job`
* **Cost Driver:** Charged per vCPU-second and GB-second of active execution.
* **Estimate:** Active vCPU costs $0.000024/sec and memory costs $0.000003/GiB-sec. Assuming an average incremental sync takes 2 minutes (120s):
  * vCPU cost: 120 * $0.000024 = $0.00288
  * Mem cost: 120 * 2 * $0.000003 = $0.00072
  * **Total per average sync = ~$0.0036**.
* **Impact:** 
  * **Free:** 1 source * 1 sync/day = ~30 syncs/mo = **$0.11 / month**.
  * **Starter:** 3 sources * 12 syncs/day (every 2 hours) = ~1,080 syncs/mo = **$3.89 / month**.
  * **Growth:** 10 sources * 144 syncs/day (every 10 mins) = ~43,200 syncs/mo = **$155.52 / month**.

### 1.4 Orchestration & Schema Updates (Azure Functions)
* **Resources:** `func-purelybi-sync-orchestrator-v2-dev-ci`, `func-purelybi-schema-updater-dev-ci`
* **Cost Driver:** Consumption Plan (per execution & GB-s).
* **Impact:** Extremely low cost. First 1 million executions are free. Estimated overhead per user is **<$0.05 / month**.

### 1.5 Storage & Container Registry (Azure Storage / ACR)
* **Resources:** `sapurelybisyncv2devci`, `sapurelybifuncv2devci`, `acrpurelybiv2devci`
* **Cost Driver:** Blob storage is ~$0.018 per GB/month in Central India. Transaction costs are negligible.
* **Impact:** 1 GB of stored analytical data costs **~$0.02 / month**.

### 1.6 Telemetry (Log Analytics Workspace & App Insights)
* **Resources:** `law-purelybi-sync-v2-dev-ci`
* **Cost Driver:** Log ingestion (~$2.30 per GB).
* **Impact:** Assuming ~20MB logs generated per user per month, this equates to **~$0.05 / month**.

### 1.7 LLM/AI Processing (Azure AI Foundry - Anthropic Sonnet 4.5)
* **Cost Driver:** Tokens consumed during dense, multi-tool agent loops.
* **Estimate:** Development testing alone generated a cost of **5,208 INR (~$62.50 USD)** for Claude Sonnet 4.5. This reveals the true cost of agentic behavior: agents iteratively execute multiple tools (Onboarding, Dashboard, SQL) spanning 10-20 turns. The compounding context window (massive inputs per turn and tool outputs) results in exponential token usage compared to simple chat interfaces.
* **Impact:** Based on dev testing, a single power user can easily consume $50 to $100 per month.
  * **Starter (200 AI Credits):** Max estimated cost is **~$20.00 / month**.
  * **Growth (1,000 AI Credits):** Max estimated cost is **~$100.00 / month**.

### Total Cost of Goods Sold (COGS) Example (Max Limits)
For a user on the **Starter** tier maxing out 3 Data Sources (every 2h), 1 GB storage, and 200 AI credits/month:
* **Backend Auto-Scale Compute:** $2.40
* **Compute (Data Syncs):** $3.89
* **Storage / Orchestration / Logs / Frontend:** $0.12
* **LLM API (Anthropic Sonnet 4.5):** $20.00
* **Total Direct COGS = ~$26.41 / month**

For a user on the **Growth** tier maxing out 10 Data Sources (every 10 mins) and 1,000 AI credits:
* **Compute (Data Syncs):** $155.52
* **LLM API:** $100.00
* **Total Direct COGS = ~$258.04 / month**

## 2. Margin Targets & Pricing Strategy

Standard B2B SaaS applications aim for a **75% - 85% gross margin**. 
Given the explosive compute cost of 10-minute syncs ($155.52/mo) and multi-tool agent loops ($100.00/mo), the originally proposed $19 and $49 price points are highly unprofitable for power users. 

To achieve standard SaaS margins (assuming a typical 30% usage rate across the user base):
* **Starter Tier** must be priced at **$49 / month** (Typical COGS: $7.92 -> 83.8% margin).
* **Pro Tier** must be priced at **$149 / month** (Typical COGS: $19.64 -> 86.8% margin).
* **Growth Tier** must be priced at **$299 / month** (Typical COGS: $77.41 -> 74.1% margin).

## 3. Subscription Feature Comparison

| Feature | Free (7-Day Trial) | Starter | Pro | Growth | Enterprise |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Price / Month** | **$0** | **$49** | **$149** | **$299** | **$999+** |
| **Data Sources** | 1 | 3 | 5 | 10 | Unlimited |
| **Storage Limit** | 100 MB | 1 GB | 5 GB | 10 GB | 100 GB+ |
| **Dashboards** | 1 | 5 | 15 | Unlimited | Unlimited |
| **Included AI Credits** | 25 | 200 | 500 | 1,000 | 5,000+ |
| **Add-on AI Credits**| No | Unlimited | Unlimited | Unlimited | Unlimited |
| **Data Sync Frequency**| 1x / Day | Every 2 Hours | Every 1 Hour | Every 10 Mins | Every 10 Mins |
| **Report Sharing** | None | None | Read-only | Read/Edit + Share data | Read/Edit + Share data |
| **Alerting** | None | None | Basic | Advanced | Advanced |

## 4. Cost vs. Profit Analysis

The following table models Monthly Recurring Revenue (MRR), total Cost of Goods Sold (COGS), and Gross Profit margins across different tier and user-base scenarios. 

*Note: COGS estimates assume a typical SaaS resource utilization rate of **30%** of the maximum allowed limits per user.*

| Scenario (Tier & Users) | Monthly Revenue (MRR) | Estimated COGS (30% Usage) | Gross Profit | Margin |
| :--- | :--- | :--- | :--- | :--- |
| **Free Trial (7 Days) - 10 Users** | $0 | $6.09 | -$6.09 | N/A |
| **Free Trial (7 Days) - 50 Users** | $0 | $30.45 | -$30.45 | N/A |
| **Free Trial (7 Days) - 100 Users** | $0 | $60.90 | -$60.90 | N/A |
| **Starter Tier - 10 Users** | $490 | $79.20 | $410.80 | 83.8% |
| **Starter Tier - 50 Users** | $2,450 | $396.00 | $2,054.00 | 83.8% |
| **Starter Tier - 100 Users** | $4,900 | $792.00 | $4,108.00 | 83.8% |
| **Pro Tier - 10 Users** | $1,490 | $196.40 | $1,293.60 | 86.8% |
| **Pro Tier - 50 Users** | $7,450 | $982.00 | $6,468.00 | 86.8% |
| **Pro Tier - 100 Users** | $14,900 | $1,964.00 | $12,936.00 | 86.8% |
| **Growth Tier - 10 Users** | $2,990 | $774.10 | $2,215.90 | 74.1% |
| **Growth Tier - 50 Users** | $14,950 | $3,870.50 | $11,079.50 | 74.1% |
| **Growth Tier - 100 Users** | $29,900 | $7,741.00 | $22,159.00 | 74.1% |
| **Enterprise - 10 Users** | $9,990 | $3,500.00 | $6,490.00 | 64.9% |

## 5. Feature Limit Enforcement Rules

1. **Free Trial:** The Free plan is limited to a 7-day trial period. After 7 days, the workspace becomes read-only until the user upgrades to a paid tier.
2. **AI Credits:** 1 AI Credit is consumed for every user message sent to the agent, regardless of how many internal tools the agent executes to resolve that message. **Users on any paid tier (Starter, Pro, Growth, Enterprise) can purchase additional AI credit packs** (e.g., $10 for 100 credits) dynamically if they exceed their monthly limit.
3. **Data Sync Rate Limits:** All tiers enforce strict minimum intervals between successful job triggers according to the plan (24h, 2h, 1h, or 10m).
4. **Storage Caps:** Storage is measured by the total size of uncompressed Parquet data within the user's isolated blob storage prefix. Limits are enforced prior to new sync jobs executing.
