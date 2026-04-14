const NAV_ITEMS = [
  { href: '/overview', label: 'Overview' },
  { href: '/inventory', label: 'Inventory' },
  { href: '/invoicing', label: 'Invoicing' },
  { href: '/backorders', label: 'Backorders' },
  { href: '/stretch', label: 'Stretch' },
]

function TopNav({ currentPath }) {
  return (
    <header className="border-b border-border bg-card">
      <nav className="mx-auto flex w-full max-w-7xl flex-wrap items-center gap-2 px-4 py-3 md:px-8">
        <span className="mr-3 text-sm font-semibold uppercase tracking-[0.2em] text-muted-foreground">
          Telemachus WMS
        </span>
        {NAV_ITEMS.map((item) => {
          const isActive =
            currentPath === item.href || (item.href === '/overview' && currentPath === '/')

          return (
            <a
              key={item.href}
              href={item.href}
              className={`px-3 py-1.5 text-sm font-medium transition ${
                isActive
                  ? 'bg-primary text-primary-foreground'
                  : 'border border-border bg-background text-foreground hover:bg-accent'
              }`}
            >
              {item.label}
            </a>
          )
        })}
      </nav>
    </header>
  )
}

export default TopNav
