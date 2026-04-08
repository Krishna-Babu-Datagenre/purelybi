# Dravya BI — Frontend

A **Business Intelligence** web app where users explore data in natural language, manage dashboards with draggable widgets (charts, KPIs), chat with an AI assistant, and work with reports. Built as a single-page React application.

**Stack:** Vite, React 19, TypeScript, Tailwind CSS, Zustand, Apache ECharts (`echarts-for-react`), `react-grid-layout`.

---

## Prerequisites

- **Node.js** 20+ (LTS recommended)
- **npm** 10+

---

## Installation

```bash
git clone <repository-url>
cd <cloned-folder>
npm ci
```

### API base URL

The app talks to a FastAPI backend via `VITE_API_BASE_URL` (see `src/services/authApi.ts`, `backendClient.ts`, `chatApi.ts`).

- **Local development:** create a `.env` or `.env.local` file in the project root:

  ```env
  VITE_API_BASE_URL=http://localhost:8000
  ```

  Adjust the port to match your API. If unset, the code defaults to `http://localhost:8000`.

- **Production:** the value is set at **build time** (e.g. in GitHub Actions for CI/CD).

---

## Scripts

| Command        | Description                    |
|----------------|--------------------------------|
| `npm run dev`  | Dev server (Vite)              |
| `npm run build`| Typecheck + production bundle  |
| `npm run preview` | Serve the `dist/` output locally |

---

## Deployment

This repository deploys to **Azure Static Web Apps** using **GitHub Actions** (`.github/workflows/azure-static-web-apps.yml`). The API URL used for production builds is configured in that workflow.

---

## Environments

| Environment | URL |
|-------------|-----|
| **Frontend (production)** | [https://jolly-bush-014efc800.1.azurestaticapps.net](https://jolly-bush-014efc800.1.azurestaticapps.net) |
| **Backend API** | [https://app-dravya-bi-backend-np-ci.azurewebsites.net](https://app-dravya-bi-backend-np-ci.azurewebsites.net) |

The backend must allow the frontend **origin** in CORS (e.g. `CORSMiddleware` in FastAPI). Update the table above if URLs change.

---

## Repository layout (high level)

```text
.
├── .github/workflows/     # CI/CD (Azure Static Web Apps)
├── public/                # Static assets copied as-is into the build (e.g. staticwebapp.config.json)
├── src/
│   ├── components/        # Shared UI (layout chrome, chat, login, …)
│   ├── layouts/           # Dashboard canvas (e.g. grid)
│   ├── widgets/           # Dashboard tiles (charts, KPIs, …)
│   ├── services/          # API clients (auth, chat, backend, dashboard messaging)
│   ├── store/             # Zustand stores
│   ├── data/              # Dummy / sample data for development
│   ├── types/             # TypeScript types
│   ├── utils/             # Helpers (layout, mapping, …)
│   ├── App.tsx            # Root shell & routing of views
│   ├── main.tsx           # Entry
│   └── index.css          # Global styles + Tailwind
├── index.html
├── vite.config.ts
└── package.json
```

Naming and responsibilities for new code are described in `AGENTS.md`.
