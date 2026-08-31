with open('src/App.tsx', 'r') as f:
    c = f.read()

import_statement = "import DataPipelinesPage from './components/data/DataPipelinesPage';\n"
if 'DataPipelinesPage' not in c:
    c = c.replace("import DataRawTablesPage from './components/data/DataRawTablesPage';", "import DataRawTablesPage from './components/data/DataRawTablesPage';\n" + import_statement)

route_cond = """          {navigationPage === 'data-raw-tables' && (
            <DataRawTablesPage
              sidebarCollapsed={sidebarCollapsed}
              chatOpen={chatOpen}
              chatModal={chatModal}
              chatWidthPx={chatWidthPx}
            />
          )}"""
          
new_route = """          {navigationPage === 'data-pipelines' && (
            <DataPipelinesPage
              sidebarCollapsed={sidebarCollapsed}
              chatOpen={chatOpen}
              chatModal={chatModal}
              chatWidthPx={chatWidthPx}
            />
          )}"""
          
if "navigationPage === 'data-pipelines'" not in c:
    c = c.replace(route_cond, route_cond + '\n' + new_route)

# Also fix topbarTitle
title_cond = "if (navigationPage === 'data-raw-tables') return 'View raw tables';"
if "if (navigationPage === 'data-pipelines') return 'Data engineering';" not in c:
    c = c.replace(title_cond, title_cond + "\n    if (navigationPage === 'data-pipelines') return 'Data engineering';")

with open('src/App.tsx', 'w') as f:
    f.write(c)
