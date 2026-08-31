import re

with open('src/store/useDashboardStore.ts', 'r') as f:
    c = f.read()
if "| 'data-pipelines'" not in c:
    c = c.replace("| 'data-raw-tables'", "| 'data-raw-tables'\n  | 'data-pipelines'")
with open('src/store/useDashboardStore.ts', 'w') as f:
    f.write(c)

with open('src/utils/routes.ts', 'r') as f:
    c = f.read()
if "data-pipelines" not in c:
    c = c.replace("'data-raw-tables': '/data/raw',", "'data-raw-tables': '/data/raw',\n  'data-pipelines': '/data/pipelines',")
    c = c.replace("if (pathname === '/data/raw') return { page: 'data-raw-tables' };", "if (pathname === '/data/raw') return { page: 'data-raw-tables' };\n  if (pathname === '/data/pipelines') return { page: 'data-pipelines' };")
with open('src/utils/routes.ts', 'w') as f:
    f.write(c)

with open('src/components/Sidebar.tsx', 'r') as f:
    c = f.read()
if "data-pipelines" not in c:
    old1 = """              <button
                type="button"
                onClick={() => setNavigationPage('data-raw-tables')}
                className={`sidebar-item sidebar-item--collapsed ${navigationPage === 'data-raw-tables' ? 'sidebar-item--active' : ''}`}
                title="View raw tables"
              >
                <Table size={20} />
              </button>"""
    new1 = """              <button
                type="button"
                onClick={() => setNavigationPage('data-pipelines')}
                className={`sidebar-item sidebar-item--collapsed ${navigationPage === 'data-pipelines' ? 'sidebar-item--active' : ''}`}
                title="Data pipelines"
              >
                <FastForward size={20} />
              </button>"""
              
    old2 = """              <button
                type="button"
                onClick={() => setNavigationPage('data-raw-tables')}
                className={`sidebar-item ${navigationPage === 'data-raw-tables' ? 'sidebar-item--active' : ''}`}
              >
                <div className="sidebar-icon">
                  <Table size={20} />
                </div>
                {!sidebarCollapsed && <span className="sidebar-label">Raw tables</span>}
              </button>"""
    new2 = """              <button
                type="button"
                onClick={() => setNavigationPage('data-pipelines')}
                className={`sidebar-item ${navigationPage === 'data-pipelines' ? 'sidebar-item--active' : ''}`}
              >
                <div className="sidebar-icon">
                  <FastForward size={20} />
                </div>
                {!sidebarCollapsed && <span className="sidebar-label">Pipelines (Advanced)</span>}
              </button>"""
              
    c = c.replace(old1, new1 + '\n' + old1)
    c = c.replace(old2, new2 + '\n' + old2)
    # also add FastForward import
    c = c.replace("Database, ", "Database, FastForward, ")

with open('src/components/Sidebar.tsx', 'w') as f:
    f.write(c)
