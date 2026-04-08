# Project Context
I am building a web application that enables users to interact with their data using natural language powered by Generative AI. Users can ask questions about their data, create visuals, build reports (including mixed visuals, KPI blocks, and more), rearrange visuals within a report, and export their reports seamlessly.

# Role
Act as an expert React, TypeScript, and Frontend Architecture developer.

# Tech Stack
- **Framework**: Vite + React
- **Language**: TypeScript (Strict mode)
- **Styling**: Tailwind CSS
- **Layout/Canvas**: `react-grid-layout`
- **Charts**: Apache ECharts via `echarts-for-react`
- **State Management**: `Zustand`
- **Icons**: `lucide-react`

# Coding Standards & Best Practices
1. **Functional Components**: Use React Functional Components and Hooks only. Never use Class components.
2. **TypeScript First**: Always define `interface` or `type` for component props, state, and dummy data. No `any` types.
3. **Modularity**: Keep components small and focused. A component should do one thing well.
4. **Tailwind Styling**: Use Tailwind utility classes for all styling. Avoid custom CSS files unless absolutely necessary for `react-grid-layout` overrides.
5. **Dummy Data**: Keep all hardcoded dummy data isolated in a separate `src/data/` folder, not directly inside the components.

# Project Folder Structure
Enforce the following folder structure when creating new files:
/src
  /components      # Reusable UI parts (buttons, cards, inputs)
  /widgets         # Dashboard specific components (ChartWidget, KPIWidget)
  /layouts         # Dashboard canvas (DashboardGrid)
  /store           # Zustand state files (useDashboardStore.ts)
  /data            # Hardcoded JSON dummy data and ECharts configs
  /types           # TypeScript interfaces (index.d.ts)
  /utils           # Helper functions