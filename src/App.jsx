import { useMemo } from 'react'
import TopNav from './components/TopNav'
import BackordersPage from './pages/BackordersPage'
import InvoicingPage from './pages/InvoicingPage'
import InventoryPage from './pages/InventoryPage'
import OverviewPage from './pages/OverviewPage'
import StretchPage from './pages/StretchPage'

function App() {
  const path = useMemo(() => {
    const normalizedPath = window.location.pathname.replace(/\/+$/, '')
    return normalizedPath || '/'
  }, [])

  let page = null

  if (path === '/' || path === '/overview') {
    page = <OverviewPage />
  } else if (path === '/inventory') {
    page = <InventoryPage />
  } else if (path === '/invoicing') {
    page = <InvoicingPage />
  } else if (path === '/backorders') {
    page = <BackordersPage />
  } else if (path === '/stretch') {
    page = <StretchPage />
  }

  return (
    <div className="min-h-screen bg-background">
      <TopNav currentPath={path} />
      {page ?? (
        <main className="mx-auto flex w-full max-w-3xl items-center justify-center p-6">
          <section className="w-full border border-border bg-card p-6 shadow">
            <h1 className="text-xl font-semibold text-foreground">Route Not Found</h1>
            <p className="mt-2 text-sm text-muted-foreground">
              Visit{' '}
              <a className="font-medium text-primary hover:underline" href="/overview">
                /overview
              </a>{' '}
              or{' '}
              <a className="font-medium text-primary hover:underline" href="/inventory">
                /inventory
              </a>
              .
            </p>
          </section>
        </main>
      )}
    </div>
  )
}

export default App
