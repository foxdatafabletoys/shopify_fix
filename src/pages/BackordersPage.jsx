import { useEffect, useMemo, useState } from 'react'

function SummaryCard({ label, value }) {
  return (
    <article className="border border-border bg-card p-5 shadow">
      <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{label}</p>
      <p className="mt-2 text-2xl font-semibold text-foreground">{value}</p>
    </article>
  )
}

function BackordersPage() {
  const [backorders, setBackorders] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let isSubscribed = true

    async function fetchBackorders() {
      setLoading(true)
      setError('')

      try {
        const response = await fetch(`/api/backorders?t=${Date.now()}`, {
          cache: 'no-store',
        })

        if (!response.ok) {
          throw new Error(`Backorders request failed (${response.status}).`)
        }

        const data = await response.json()
        if (!isSubscribed) return
        setBackorders(Array.isArray(data) ? data : [])
      } catch (fetchError) {
        if (!isSubscribed) return
        setError(
          fetchError instanceof Error ? fetchError.message : 'Failed to load backorders.',
        )
      } finally {
        if (isSubscribed) setLoading(false)
      }
    }

    fetchBackorders()

    return () => {
      isSubscribed = false
    }
  }, [])

  const gbpFormatter = useMemo(
    () =>
      new Intl.NumberFormat('en-GB', {
        style: 'currency',
        currency: 'GBP',
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }),
    [],
  )

  const grouped = useMemo(() => {
    const groups = new Map()
    for (const row of backorders) {
      if (!groups.has(row.sku)) {
        groups.set(row.sku, {
          sku: row.sku,
          description: row.description,
          totalBackordered: 0,
          customers: [],
        })
      }
      const group = groups.get(row.sku)
      group.totalBackordered += row.qty_backordered
      group.customers.push(row)
    }
    return Array.from(groups.values())
  }, [backorders])

  const summary = useMemo(() => {
    const uniqueSkus = new Set(backorders.map((r) => r.sku))
    const uniqueCustomers = new Set(backorders.map((r) => r.customer_id).filter(Boolean))
    const totalUnits = backorders.reduce((sum, r) => sum + r.qty_backordered, 0)
    const totalValue = backorders.reduce(
      (sum, r) => sum + r.qty_backordered * Number(r.trade_price_gbp),
      0,
    )
    return {
      skuCount: uniqueSkus.size,
      totalUnits,
      customerCount: uniqueCustomers.size,
      totalValue,
    }
  }, [backorders])

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
        <section className="border border-border bg-sidebar p-6 shadow md:p-8">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
            Stock Allocation
          </p>
          <h1 className="mt-2 text-3xl font-semibold text-sidebar-primary md:text-4xl">
            Backorders
          </h1>
        </section>
        <section className="mt-6 border border-border bg-card p-6 text-sm text-muted-foreground shadow">
          Loading backorder data...
        </section>
      </main>
    )
  }

  if (error) {
    return (
      <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
        <section className="border border-border bg-sidebar p-6 shadow md:p-8">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
            Stock Allocation
          </p>
          <h1 className="mt-2 text-3xl font-semibold text-sidebar-primary md:text-4xl">
            Backorders
          </h1>
        </section>
        <section className="mt-6 border border-border bg-card p-6 text-sm text-destructive shadow">
          {error}
        </section>
      </main>
    )
  }

  return (
    <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
      <section className="border border-border bg-sidebar p-6 shadow md:p-8">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
          Stock Allocation
        </p>
        <h1 className="mt-2 text-3xl font-semibold text-sidebar-primary md:text-4xl">Backorders</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          Items where demand exceeds available stock. Customers ranked by loyalty (total spend).
        </p>
      </section>

      <section className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <SummaryCard
          label="Backordered SKUs"
          value={summary.skuCount.toLocaleString('en-GB')}
        />
        <SummaryCard
          label="Backordered Units"
          value={summary.totalUnits.toLocaleString('en-GB')}
        />
        <SummaryCard
          label="Customers Waiting"
          value={summary.customerCount.toLocaleString('en-GB')}
        />
        <SummaryCard
          label="Backordered Value"
          value={gbpFormatter.format(summary.totalValue)}
        />
      </section>

      {grouped.length === 0 ? (
        <section className="mt-6 border border-border bg-card p-6 shadow">
          <p className="text-sm text-muted-foreground">
            No backorders. All invoiced items are fully allocated from available stock.
          </p>
        </section>
      ) : (
        <section className="mt-6 space-y-4">
          {grouped.map((group) => (
            <div key={group.sku} className="border border-border bg-card shadow">
              <div className="flex items-center justify-between bg-muted px-4 py-3">
                <div>
                  <span className="font-mono text-xs font-semibold text-foreground">
                    {group.sku}
                  </span>
                  <span className="ml-3 text-sm text-muted-foreground">{group.description}</span>
                </div>
                <span className="text-xs font-semibold text-destructive">
                  {group.totalBackordered} unit{group.totalBackordered !== 1 ? 's' : ''} backordered
                </span>
              </div>
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-border">
                  <thead className="text-left text-xs uppercase tracking-wide text-muted-foreground">
                    <tr>
                      <th className="px-4 py-2">Priority</th>
                      <th className="px-4 py-2">Customer</th>
                      <th className="px-4 py-2">Region</th>
                      <th className="px-4 py-2 text-right">Loyalty (Spend)</th>
                      <th className="px-4 py-2 text-right">Qty Fulfilled</th>
                      <th className="px-4 py-2 text-right">Qty Backordered</th>
                      <th className="px-4 py-2">Status</th>
                      <th className="px-4 py-2">Order #</th>
                      <th className="px-4 py-2">Date</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border text-sm">
                    {group.customers.map((row, index) => (
                      <tr key={row.line_id} className="hover:bg-accent">
                        <td className="px-4 py-2">
                          <span className="inline-flex h-6 w-6 items-center justify-center bg-primary text-xs font-semibold text-primary-foreground">
                            {index + 1}
                          </span>
                        </td>
                        <td className="px-4 py-2 font-medium">{row.company_name || 'Unknown'}</td>
                        <td className="px-4 py-2 uppercase">{row.region || '-'}</td>
                        <td className="px-4 py-2 text-right font-mono text-xs">
                          {gbpFormatter.format(row.customer_total_spend)}
                        </td>
                        <td className="px-4 py-2 text-right">{row.qty_fulfilled}</td>
                        <td className="px-4 py-2 text-right font-semibold text-destructive">
                          {row.qty_backordered}
                        </td>
                        <td className="px-4 py-2">
                          <span
                            className={`inline-flex px-2 py-0.5 text-xs font-semibold ${
                              row.fulfilment_status === 'partial'
                                ? 'bg-accent text-accent-foreground'
                                : 'bg-destructive/10 text-destructive'
                            }`}
                          >
                            {row.fulfilment_status === 'partial' ? 'Partial' : 'Backordered'}
                          </span>
                        </td>
                        <td className="px-4 py-2 font-mono text-xs">
                          {row.order_number || '-'}
                        </td>
                        <td className="px-4 py-2 text-xs text-muted-foreground">
                          {row.invoice_date || '-'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </section>
      )}
    </main>
  )
}

export default BackordersPage
