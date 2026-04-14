import { useEffect, useMemo, useState } from 'react'
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'

const RISK_STYLES = {
  healthy: 'bg-emerald-100 text-emerald-900',
  due_soon: 'bg-amber-100 text-amber-900',
  lapsed: 'bg-red-100 text-red-900',
}

function InfoTip({ text }) {
  return (
    <span className="group relative ml-1.5 inline-block cursor-help align-middle">
      <svg
        xmlns="http://www.w3.org/2000/svg"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        className="h-3.5 w-3.5 text-muted-foreground"
      >
        <circle cx="12" cy="12" r="10" />
        <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
        <line x1="12" y1="17" x2="12.01" y2="17" />
      </svg>
      <span className="pointer-events-none absolute bottom-full left-1/2 z-50 mb-2 hidden w-64 -translate-x-1/2 border border-border bg-popover p-3 text-xs font-normal normal-case tracking-normal leading-relaxed text-popover-foreground shadow-lg backdrop-blur-sm group-hover:block">
        {text}
      </span>
    </span>
  )
}

function toNumber(value) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function formatDateText(value) {
  if (!value) {
    return '-'
  }

  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    return '-'
  }

  return parsed.toLocaleDateString('en-GB')
}

function getRiskLabel(riskStatus) {
  if (riskStatus === 'lapsed') {
    return 'Lapsed'
  }

  if (riskStatus === 'due_soon') {
    return 'Due Soon'
  }

  return 'Healthy'
}

function SummaryCard({ label, value }) {
  return (
    <article className="border border-border bg-card p-5 shadow">
      <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{label}</p>
      <p className="mt-2 text-2xl font-semibold text-foreground">{value}</p>
    </article>
  )
}

function StretchPage() {
  const [data, setData] = useState(null)
  const [backorders, setBackorders] = useState([])
  const [customerHealth, setCustomerHealth] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let isSubscribed = true

    async function fetchData() {
      setLoading(true)
      setError('')

      try {
        const [summaryRes, backordersRes, customerHealthRes] = await Promise.all([
          fetch(`/api/stretch/summary?t=${Date.now()}`, { cache: 'no-store' }),
          fetch(`/api/backorders?t=${Date.now()}`, { cache: 'no-store' }),
          fetch(`/api/stretch/customer-health?t=${Date.now()}`, { cache: 'no-store' }),
        ])

        if (!summaryRes.ok) {
          throw new Error(`Stretch summary failed (${summaryRes.status}).`)
        }

        const json = await summaryRes.json()
        const boData = backordersRes.ok ? await backordersRes.json() : []
        const healthData = customerHealthRes.ok ? await customerHealthRes.json() : []
        if (!isSubscribed) return
        setData(json)
        setBackorders(Array.isArray(boData) ? boData : [])
        setCustomerHealth(Array.isArray(healthData) ? healthData : [])
      } catch (fetchError) {
        if (!isSubscribed) return
        setError(fetchError instanceof Error ? fetchError.message : 'Failed to load stretch data.')
      } finally {
        if (isSubscribed) setLoading(false)
      }
    }

    fetchData()

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

  const dateRange = useMemo(() => {
    if (!data?.dateRange?.earliest) return 'No data'
    if (data.dateRange.earliest === data.dateRange.latest) return data.dateRange.earliest
    return `${data.dateRange.earliest} to ${data.dateRange.latest}`
  }, [data])

  const chartData = useMemo(() => {
    if (!data?.revenueByMonth) return []
    return data.revenueByMonth.map((m) => ({
      month: m.month,
      revenue: Number(m.revenue),
      units: m.units,
    }))
  }, [data])

  const backorderProducts = useMemo(() => {
    const groups = new Map()
    for (const row of backorders) {
      if (!groups.has(row.sku)) {
        groups.set(row.sku, {
          sku: row.sku,
          description: row.description,
          totalBackordered: 0,
          customerCount: 0,
        })
      }
      const g = groups.get(row.sku)
      g.totalBackordered += row.qty_backordered
      g.customerCount += 1
    }
    return Array.from(groups.values())
  }, [backorders])

  const topProductsChart = useMemo(() => {
    if (!data?.topProducts) return []
    return data.topProducts.map((p) => ({
      name: p.description?.length > 28 ? `${p.description.slice(0, 28)}...` : p.description || p.sku,
      units: p.total_units,
      revenue: Number(p.total_revenue),
    }))
  }, [data])

  const rankedAtRiskCustomers = useMemo(() => {
    return customerHealth
      .filter((customer) => customer.risk_status === 'lapsed' || customer.risk_status === 'due_soon')
      .map((customer) => {
        const invoiceCount = Math.max(1, toNumber(customer.invoice_count))
        const totalRevenue = toNumber(customer.total_revenue_gbp)
        const avgReorderDays = Math.max(1, Math.round(toNumber(customer.avg_reorder_days)))
        const daysSinceLastInvoice = Math.max(0, toNumber(customer.days_since_last_invoice))
        const daysOverdue = Math.max(0, daysSinceLastInvoice - avgReorderDays)
        const avgOrderValue = totalRevenue / invoiceCount
        const annualizedRevenue = avgOrderValue * (365 / avgReorderDays)

        return {
          ...customer,
          invoiceCount,
          totalRevenue,
          avgReorderDays,
          daysSinceLastInvoice,
          daysOverdue,
          annualizedRevenue,
        }
      })
      .sort((left, right) => {
        const leftRank = toNumber(left.priority_rank)
        const rightRank = toNumber(right.priority_rank)
        if (leftRank !== rightRank) {
          return leftRank - rightRank
        }
        return right.totalRevenue - left.totalRevenue
      })
  }, [customerHealth])

  const customerHealthSummary = useMemo(() => {
    let lapsedCount = 0
    let dueSoonCount = 0
    let annualizedRevenueAtRisk = 0

    for (const customer of rankedAtRiskCustomers) {
      if (customer.risk_status === 'lapsed') {
        lapsedCount += 1
      }

      if (customer.risk_status === 'due_soon') {
        dueSoonCount += 1
      }

      annualizedRevenueAtRisk += customer.annualizedRevenue
    }

    return {
      lapsedCount,
      dueSoonCount,
      annualizedRevenueAtRisk,
    }
  }, [rankedAtRiskCustomers])

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
        <section className="border border-border bg-sidebar p-6 shadow md:p-8">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
            Sales Intelligence
          </p>
          <h1 className="mt-2 text-3xl font-semibold text-sidebar-primary md:text-4xl">Stretch</h1>
        </section>
        <section className="mt-6 border border-border bg-card p-6 text-sm text-muted-foreground shadow">
          Loading stretch data...
        </section>
      </main>
    )
  }

  if (error) {
    return (
      <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
        <section className="border border-border bg-sidebar p-6 shadow md:p-8">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
            Sales Intelligence
          </p>
          <h1 className="mt-2 text-3xl font-semibold text-sidebar-primary md:text-4xl">Stretch</h1>
        </section>
        <section className="mt-6 border border-border bg-card p-6 text-sm text-destructive shadow">
          {error}
        </section>
      </main>
    )
  }

  if (!data || data.totalInvoices === 0) {
    return (
      <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
        <section className="border border-border bg-sidebar p-6 shadow md:p-8">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
            Sales Intelligence
          </p>
          <h1 className="mt-2 text-3xl font-semibold text-sidebar-primary md:text-4xl">Stretch</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Accumulates invoice data to surface sales velocity, customer geography, and warehouse
            alignment insights.
          </p>
        </section>
        <section className="mt-6 border border-border bg-card p-6 shadow">
          <p className="text-sm text-muted-foreground">
            No invoices saved yet. Head to{' '}
            <a href="/invoicing" className="font-medium text-primary hover:underline">
              /invoicing
            </a>{' '}
            to upload an order form and click <strong>Save to Stretch</strong> to start tracking.
          </p>
        </section>
      </main>
    )
  }

  return (
    <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
      <section className="border border-border bg-sidebar p-6 shadow md:p-8">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
          Sales Intelligence
        </p>
        <h1 className="mt-2 text-3xl font-semibold text-sidebar-primary md:text-4xl">Stretch</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          {data.totalInvoices} invoice{data.totalInvoices !== 1 ? 's' : ''} tracked. More data =
          richer insights.
        </p>
      </section>

      {/* Summary Cards */}
      <section className="mt-6">
        <p className="mb-3 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Portfolio Overview
          <InfoTip text="High-level pulse of the business. Each invoice saved from the Invoicing tab feeds these numbers. The more invoices you track, the more accurate the picture. Revenue is in GBP at trade price." />
        </p>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <SummaryCard label="Total Invoices" value={data.totalInvoices.toLocaleString('en-GB')} />
          <SummaryCard label="Total Revenue" value={gbpFormatter.format(data.totalRevenue)} />
          <SummaryCard label="Total Units Sold" value={data.totalUnits.toLocaleString('en-GB')} />
          <SummaryCard label="Date Range" value={dateRange} />
        </div>
      </section>

      <section className="mt-6 border border-border bg-card p-4 shadow md:p-6">
        <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Customer Health
          <InfoTip text="Tracks each customer's average reorder interval. If they haven't ordered in longer than their usual cycle, they're flagged as 'due soon' or 'lapsed'. Ranked by annualised revenue so you call the most valuable accounts first. The system learns each customer's rhythm from their invoice history." />
        </p>
        <p className="mt-1 text-xs text-muted-foreground">
          Reorder-risk ranking for repeat B2B customers, prioritised by account value.
        </p>

        <div className="mt-4 grid gap-4 md:grid-cols-3">
          <SummaryCard
            label="Lapsed Accounts"
            value={customerHealthSummary.lapsedCount.toLocaleString('en-GB')}
          />
          <SummaryCard
            label="Due Soon"
            value={customerHealthSummary.dueSoonCount.toLocaleString('en-GB')}
          />
          <SummaryCard
            label="Revenue At Risk (Annualized)"
            value={gbpFormatter.format(customerHealthSummary.annualizedRevenueAtRisk)}
          />
        </div>

        {rankedAtRiskCustomers.length > 0 ? (
          <div className="mt-4 overflow-x-auto">
            <table className="min-w-full divide-y divide-border">
              <thead className="text-left text-xs uppercase tracking-wide text-muted-foreground">
                <tr>
                  <th className="px-3 py-2">Priority</th>
                  <th className="px-3 py-2">Customer</th>
                  <th className="px-3 py-2">Risk</th>
                  <th className="px-3 py-2">Last Invoice</th>
                  <th className="px-3 py-2">Expected Reorder</th>
                  <th className="px-3 py-2 text-right">Days Overdue</th>
                  <th className="px-3 py-2 text-right">Lifetime Revenue</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border text-sm">
                {rankedAtRiskCustomers.map((customer) => (
                  <tr key={customer.customer_id} className="hover:bg-accent">
                    <td className="px-3 py-2 font-mono text-xs">{customer.priority_rank}</td>
                    <td className="px-3 py-2 font-medium">{customer.company_name}</td>
                    <td className="px-3 py-2">
                      <span
                        className={`inline-flex px-2 py-0.5 text-xs font-semibold ${
                          RISK_STYLES[customer.risk_status] ?? 'bg-muted text-foreground'
                        }`}
                      >
                        {getRiskLabel(customer.risk_status)}
                      </span>
                    </td>
                    <td className="px-3 py-2">{formatDateText(customer.last_invoice_date)}</td>
                    <td className="px-3 py-2">{formatDateText(customer.expected_next_invoice_date)}</td>
                    <td className="px-3 py-2 text-right font-semibold text-destructive">
                      {customer.daysOverdue.toLocaleString('en-GB')}
                    </td>
                    <td className="px-3 py-2 text-right">
                      {gbpFormatter.format(customer.totalRevenue)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="mt-4 text-sm text-muted-foreground">
            No at-risk reorder accounts yet. Customer health updates after each new invoice.
          </p>
        )}

        <p className="mt-4 text-xs text-muted-foreground">
          Action cue: call lapsed accounts first, then due-soon accounts with volume or replenishment offers.
        </p>
      </section>

      {/* Revenue Over Time */}
      {chartData.length > 0 ? (
        <section className="mt-6 border border-border bg-card p-4 shadow md:p-6">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            Revenue Over Time
            <InfoTip text="Monthly revenue trend built from invoice dates. Shows seasonality and growth direction. The more months of data, the clearer the pattern. Useful for spotting whether a quiet month is normal or a problem." />
          </p>
          <div className="mt-4" style={{ width: '100%', height: 280 }}>
            <ResponsiveContainer>
              <AreaChart data={chartData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                <XAxis
                  dataKey="month"
                  tick={{ fontSize: 11, fontFamily: 'Geist Mono, monospace' }}
                  stroke="hsl(var(--muted-foreground))"
                />
                <YAxis
                  tick={{ fontSize: 11, fontFamily: 'Geist Mono, monospace' }}
                  stroke="hsl(var(--muted-foreground))"
                  tickFormatter={(v) => `\u00A3${(v / 1000).toFixed(0)}k`}
                />
                <Tooltip
                  contentStyle={{
                    fontFamily: 'Geist Mono, monospace',
                    fontSize: 12,
                    border: '1px solid hsl(var(--border))',
                    background: 'hsl(var(--popover))',
                    color: 'hsl(var(--popover-foreground))',
                  }}
                  formatter={(value) => [gbpFormatter.format(value), 'Revenue']}
                />
                <Area
                  type="monotone"
                  dataKey="revenue"
                  stroke="hsl(var(--chart-1))"
                  fill="hsl(var(--chart-1))"
                  fillOpacity={0.15}
                  strokeWidth={2}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </section>
      ) : null}

      {/* Top / Bottom Products */}
      <section className="mt-6 grid gap-6 lg:grid-cols-2">
        {/* Top Sellers */}
        <div className="border border-border bg-card p-4 shadow md:p-6">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            Top Sellers (by units)
            <InfoTip text="Ranked by total units sold across all invoices. These are your bread-and-butter products — the ones customers keep reordering. If a top seller shows low available stock on the Inventory tab, it needs restocking urgently." />
          </p>
          {topProductsChart.length > 0 ? (
            <div className="mt-4" style={{ width: '100%', height: Math.max(topProductsChart.length * 36, 120) }}>
              <ResponsiveContainer>
                <BarChart
                  data={topProductsChart}
                  layout="vertical"
                  margin={{ top: 0, right: 10, left: 0, bottom: 0 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" horizontal={false} />
                  <XAxis
                    type="number"
                    tick={{ fontSize: 10, fontFamily: 'Geist Mono, monospace' }}
                    stroke="hsl(var(--muted-foreground))"
                  />
                  <YAxis
                    type="category"
                    dataKey="name"
                    width={180}
                    tick={{ fontSize: 10, fontFamily: 'Geist Mono, monospace' }}
                    stroke="hsl(var(--muted-foreground))"
                  />
                  <Tooltip
                    contentStyle={{
                      fontFamily: 'Geist Mono, monospace',
                      fontSize: 12,
                      border: '1px solid hsl(var(--border))',
                      background: 'hsl(var(--popover))',
                      color: 'hsl(var(--popover-foreground))',
                    }}
                    formatter={(value, name) => [
                      name === 'units' ? `${value} units` : gbpFormatter.format(value),
                      name === 'units' ? 'Units' : 'Revenue',
                    ]}
                  />
                  <Bar dataKey="units" fill="hsl(var(--chart-2))" radius={[0, 2, 2, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <p className="mt-4 text-sm text-muted-foreground">No product data yet.</p>
          )}
        </div>

        {/* Slow Movers */}
        <div className="border border-border bg-card p-4 shadow md:p-6">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            Slow Movers (lowest units)
            <InfoTip text="Products with the fewest units sold. Could be new additions that haven't had time to sell, niche items, or dead stock. Compare against available inventory — if you're holding lots of a slow mover, consider promotions or discontinuing." />
          </p>
          {data.bottomProducts?.length > 0 ? (
            <div className="mt-4 overflow-x-auto">
              <table className="min-w-full divide-y divide-border">
                <thead className="text-left text-xs uppercase tracking-wide text-muted-foreground">
                  <tr>
                    <th className="px-3 py-2">SKU</th>
                    <th className="px-3 py-2">Description</th>
                    <th className="px-3 py-2 text-right">Units</th>
                    <th className="px-3 py-2 text-right">Revenue</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border text-sm">
                  {data.bottomProducts.map((p) => (
                    <tr key={p.sku} className="hover:bg-accent">
                      <td className="px-3 py-2 font-mono text-xs">{p.sku}</td>
                      <td className="px-3 py-2">{p.description || '-'}</td>
                      <td className="px-3 py-2 text-right">{p.total_units}</td>
                      <td className="px-3 py-2 text-right">{gbpFormatter.format(p.total_revenue)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="mt-4 text-sm text-muted-foreground">No product data yet.</p>
          )}
        </div>
      </section>

      {/* Customer & Geography */}
      <section className="mt-6 grid gap-6 lg:grid-cols-2">
        {/* Customer Breakdown */}
        <div className="border border-border bg-card p-4 shadow md:p-6">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            Customers
            <InfoTip text="Every customer who has had an invoice saved, ranked by total revenue. Region is set when saving the invoice. Use this to see who your biggest accounts are and where your revenue concentrates geographically." />
          </p>
          {data.customerBreakdown?.length > 0 ? (
            <div className="mt-4 overflow-x-auto">
              <table className="min-w-full divide-y divide-border">
                <thead className="text-left text-xs uppercase tracking-wide text-muted-foreground">
                  <tr>
                    <th className="px-3 py-2">Customer</th>
                    <th className="px-3 py-2">Region</th>
                    <th className="px-3 py-2 text-right">Invoices</th>
                    <th className="px-3 py-2 text-right">Revenue</th>
                    <th className="px-3 py-2 text-right">Units</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border text-sm">
                  {data.customerBreakdown.map((c) => (
                    <tr key={c.company_name} className="hover:bg-accent">
                      <td className="px-3 py-2 font-medium">{c.company_name}</td>
                      <td className="px-3 py-2 uppercase">{c.region || '-'}</td>
                      <td className="px-3 py-2 text-right">{c.invoice_count}</td>
                      <td className="px-3 py-2 text-right">{gbpFormatter.format(c.total_revenue)}</td>
                      <td className="px-3 py-2 text-right">{c.total_units}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="mt-4 text-sm text-muted-foreground">No customer data yet.</p>
          )}
        </div>

        {/* Backorder Alerts */}
        <div className="border border-border bg-card p-4 shadow md:p-6">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            On Backorder
            <InfoTip text="When an invoice is saved, the system tries to reserve stock from the matching warehouse. If there isn't enough, the shortfall is backordered. This shows which products are short and how many customers are waiting. See the Backorders tab for the full priority breakdown by customer loyalty." />
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            Products where demand exceeds available stock.
          </p>
          {backorderProducts.length > 0 ? (
            <div className="mt-4 overflow-x-auto">
              <table className="min-w-full divide-y divide-border">
                <thead className="text-left text-xs uppercase tracking-wide text-muted-foreground">
                  <tr>
                    <th className="px-3 py-2">SKU</th>
                    <th className="px-3 py-2">Description</th>
                    <th className="px-3 py-2 text-right">Units Short</th>
                    <th className="px-3 py-2 text-right">Customers</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border text-sm">
                  {backorderProducts.map((p) => (
                    <tr key={p.sku} className="hover:bg-accent">
                      <td className="px-3 py-2 font-mono text-xs">{p.sku}</td>
                      <td className="px-3 py-2">{p.description || '-'}</td>
                      <td className="px-3 py-2 text-right font-semibold text-destructive">
                        {p.totalBackordered}
                      </td>
                      <td className="px-3 py-2 text-right">{p.customerCount}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="mt-4 text-sm text-muted-foreground">
              No backorders. All demand is covered by available stock.
            </p>
          )}
        </div>
      </section>
    </main>
  )
}

export default StretchPage
