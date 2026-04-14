import { useEffect, useMemo, useState } from 'react'

function toNumber(value) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function SummaryCard({ label, value }) {
  return (
    <article className="border border-border bg-card p-5 shadow">
      <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{label}</p>
      <p className="mt-2 text-2xl font-semibold text-foreground">{value}</p>
    </article>
  )
}

async function readJsonResponse(response) {
  const contentType = response.headers.get('content-type') ?? ''
  const bodyText = await response.text()

  if (!response.ok) {
    throw new Error(`Overview request failed (${response.status}).`)
  }

  if (!contentType.toLowerCase().includes('application/json')) {
    const preview = bodyText.slice(0, 80).replace(/\s+/g, ' ')
    throw new Error(`Overview API returned non-JSON response: ${preview}`)
  }

  return JSON.parse(bodyText)
}

function OverviewPage() {
  const [inventory, setInventory] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let isSubscribed = true

    async function fetchInventory() {
      setLoading(true)
      setError('')

      try {
        const response = await fetch(`/api/inventory?t=${Date.now()}`, {
          cache: 'no-store',
        })
        const data = await readJsonResponse(response)

        if (!Array.isArray(data)) {
          throw new Error('Overview data format is invalid.')
        }

        if (!isSubscribed) {
          return
        }

        setInventory(data)
      } catch (fetchError) {
        if (!isSubscribed) {
          return
        }

        const errorMessage =
          fetchError instanceof Error ? fetchError.message : 'Failed to load overview.'
        setError(errorMessage)
        setInventory([])
      } finally {
        if (isSubscribed) {
          setLoading(false)
        }
      }
    }

    fetchInventory()

    return () => {
      isSubscribed = false
    }
  }, [])


  const summary = useMemo(() => {
    const initial = {
      skuCount: inventory.length,
      totalUnits: 0,
      outOfStockCount: 0,
      marginSum: 0,
      ukStockedCount: 0,
      usStockedCount: 0,
    }

    return inventory.reduce((accumulator, product) => {
      const cost = toNumber(product.cost_price_gbp)
      const sell = toNumber(product.sell_price_gbp)
      const margin = sell > 0 ? ((sell - cost) / sell) * 100 : 0
      accumulator.marginSum += margin

      const warehouseRows = Array.isArray(product.warehouse_stock) ? product.warehouse_stock : []
      const ukQty = toNumber(
        warehouseRows.find((stockRow) => stockRow.warehouse === 'uk')?.qty_on_hand,
      )
      const usQty = toNumber(
        warehouseRows.find((stockRow) => stockRow.warehouse === 'us')?.qty_on_hand,
      )

      if (ukQty > 0) {
        accumulator.ukStockedCount += 1
      }

      if (usQty > 0) {
        accumulator.usStockedCount += 1
      }

      const totalQty = ukQty + usQty
      accumulator.totalUnits += totalQty
      if (totalQty === 0) {
        accumulator.outOfStockCount += 1
      }

      return accumulator
    }, initial)
  }, [inventory])

  const averageMargin = summary.skuCount > 0 ? summary.marginSum / summary.skuCount : 0

  return (
    <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
      <section className="border border-border bg-sidebar p-6 shadow md:p-8">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
          Warehouse Dashboard
        </p>
        <h1 className="mt-2 text-3xl font-semibold text-sidebar-primary md:text-4xl">Overview</h1>
        <p className="mt-2 max-w-3xl text-sm text-muted-foreground">
          This is the operating model: get rid of manual order handling, keep margins visible, and scale
          B2B order volume without hiring just to process spreadsheets.
        </p>
      </section>

      {loading ? (
        <section className="mt-6 border border-border bg-card p-6 text-sm text-muted-foreground shadow">
          Loading overview data...
        </section>
      ) : error ? (
        <section className="mt-6 border border-border bg-card p-6 text-sm text-destructive shadow">
          Overview data error: {error}
        </section>
      ) : (
        <section className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <SummaryCard label="Total SKUs" value={summary.skuCount.toLocaleString('en-GB')} />
          <SummaryCard label="Total Units" value={summary.totalUnits.toLocaleString('en-GB')} />
          <SummaryCard label="Average Gross Margin" value={`${averageMargin.toFixed(1)}%`} />
          <SummaryCard
            label="Out Of Stock SKUs"
            value={summary.outOfStockCount.toLocaleString('en-GB')}
          />
        </section>
      )}

      <section className="mt-6 grid gap-4 lg:grid-cols-3">
        <article className="border border-border bg-card p-5 shadow">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">Input Layer</h2>
          <p className="mt-3 text-sm text-foreground">
            Input is simple: product list, cost, and price. Raw data lives in Inventory.
          </p>
          <p className="mt-2 text-sm text-foreground">
            Next step is obvious: edit prices directly here and add new products here, so we can leave the
            Excel sheet behind permanently.
          </p>
          <a
            href="/inventory"
            className="mt-4 inline-block border border-border bg-secondary px-3 py-2 text-sm font-medium text-secondary-foreground transition hover:bg-accent hover:text-accent-foreground"
          >
            Open Inventory
          </a>
        </article>

        <article className="border border-border bg-card p-5 shadow">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Command Center
          </h2>
          <p className="mt-3 text-sm text-foreground">
            Invoicing is the command center. You can upload a structured order file, manually search/add
            quantities, or paste messy docs and use the LLM parser to structure them fast.
          </p>
          <p className="mt-2 text-sm text-foreground">
            Today the market flow is: customer emails Excel, someone checks stock manually, someone sends
            an invoice. Human bottlenecks at every step. This is step one in removing that human choke point.
          </p>
          <a
            href="/invoicing"
            className="mt-4 inline-block border border-border bg-secondary px-3 py-2 text-sm font-medium text-secondary-foreground transition hover:bg-accent hover:text-accent-foreground"
          >
            Open Invoicing
          </a>
        </article>

        <article className="border border-border bg-card p-5 shadow">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Intelligence Layer
          </h2>
          <p className="mt-3 text-sm text-foreground">
            Clicking Save to Stretch does a lot under the surface. You get top sellers vs slow movers,
            customer performance, and reorder/churn visibility.
          </p>
          <p className="mt-2 text-sm text-foreground">
            Backorders then routes scarce units to the right customers first, ranked by loyalty and spend.
          </p>
          <div className="mt-4 flex flex-wrap gap-2">
            <a
              href="/stretch"
              className="inline-block border border-border bg-secondary px-3 py-2 text-sm font-medium text-secondary-foreground transition hover:bg-accent hover:text-accent-foreground"
            >
              Open Stretch
            </a>
            <a
              href="/backorders"
              className="inline-block border border-border bg-secondary px-3 py-2 text-sm font-medium text-secondary-foreground transition hover:bg-accent hover:text-accent-foreground"
            >
              Open Backorders
            </a>
          </div>
        </article>
      </section>

      <section className="mt-6 border border-border bg-card p-5 shadow">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">Scale Thesis</h2>
        <p className="mt-3 text-sm text-foreground">
          <strong>Nobody can find you or order without emailing an Excel file.</strong> That means one
          person ends up doing pure order admin all day.
        </p>
        <p className="mt-2 text-sm text-foreground">
          At GBP 3,700 average order value, GBP 1M is roughly 270 orders a year (about 5 per week). Sounds
          easy until every order is manual.
        </p>
        <p className="mt-2 text-sm text-foreground">
          Remove humans from the repetitive parts and the same team can process materially more orders. The
          direction is full self-serve ordering: customers place orders directly, stock updates automatically,
          and backorders log automatically.
        </p>
      </section>

      <section className="mt-6 grid gap-4 md:grid-cols-2">
        <article className="border border-border bg-card p-5 shadow">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Warehouse Coverage
          </h2>
          <p className="mt-3 text-sm text-foreground">
            UK stocked products: <strong>{summary.ukStockedCount}</strong>
          </p>
          <p className="mt-1 text-sm text-foreground">
            US stocked products: <strong>{summary.usStockedCount}</strong>
          </p>
        </article>

        <article className="border border-border bg-card p-5 shadow">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Operating Direction
          </h2>
          <p className="mt-3 text-sm text-foreground">
            Phase 1: structure orders and invoice flow. Phase 2: track reorder risk and churn so good
            accounts do not go quiet. Phase 3: full self-serve wholesale ordering.
          </p>
          <p className="mt-2 text-sm text-muted-foreground">Fun fun fun fun.</p>
        </article>
      </section>
    </main>
  )
}

export default OverviewPage
