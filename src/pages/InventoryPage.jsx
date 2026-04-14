import { useEffect, useMemo, useState } from 'react'
import { GlobeBars } from '../components/ui/cobe-globe-bars'

const WAREHOUSES = [
  { key: 'uk', label: 'UK' },
  { key: 'us', label: 'US' },
]

const CACHED_USD_RATE = 1.33957 // Rate baked into last spreadsheet export

const FALLBACK_CURRENCIES = {
  GBP: { code: 'GBP', rate: 1, fxText: '1 GBP = 1.00 GBP' },
  USD: { code: 'USD', rate: 1.27, fxText: '1 GBP = 1.27 USD (fallback)' },
  EUR: { code: 'EUR', rate: 1.17, fxText: '1 GBP = 1.17 EUR (fallback)' },
}

const STATUS_LABELS = {
  healthy: 'Healthy',
  low: 'Low',
  out: 'Out',
}

const STATUS_CLASSES = {
  healthy: 'bg-muted text-foreground',
  low: 'bg-accent text-accent-foreground',
  out: 'bg-destructive/10 text-destructive',
}

const BASE_REORDER_POINT = 10

function toNumber(value) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function getStockStatus(totalQty, reorderPointTotal) {
  if (totalQty === 0) {
    return 'out'
  }

  if (totalQty <= reorderPointTotal) {
    return 'low'
  }

  return 'healthy'
}

function normalizeProduct(product) {
  const stockByWarehouse = {
    uk: 0,
    us: 0,
  }
  const reservedByWarehouse = {
    uk: 0,
    us: 0,
  }

  let reorderPointTotal = 0
  let hasReorderPoint = false

  for (const stockRow of product.warehouse_stock ?? []) {
    const warehouse = stockRow.warehouse
    if (!warehouse || !(warehouse in stockByWarehouse)) {
      continue
    }

    stockByWarehouse[warehouse] = toNumber(stockRow.qty_on_hand)
    reservedByWarehouse[warehouse] = toNumber(stockRow.qty_reserved)
    if (stockRow.reorder_point !== null && stockRow.reorder_point !== undefined) {
      reorderPointTotal += toNumber(stockRow.reorder_point)
      hasReorderPoint = true
    }
  }

  const costPriceGbp = toNumber(product.cost_price_gbp)
  const sellPriceGbp = toNumber(product.sell_price_gbp)
  const grossProfitGbp = sellPriceGbp - costPriceGbp
  const grossMarginPct = sellPriceGbp > 0 ? (grossProfitGbp / sellPriceGbp) * 100 : 0
  const totalQty = Object.values(stockByWarehouse).reduce((sum, qty) => sum + qty, 0)
  const totalReserved = Object.values(reservedByWarehouse).reduce((sum, qty) => sum + qty, 0)
  const totalAvailable = totalQty - totalReserved
  const effectiveReorderPoint = hasReorderPoint ? reorderPointTotal : BASE_REORDER_POINT
  const stockStatus = getStockStatus(totalAvailable, effectiveReorderPoint)

  return {
    id: product.id,
    name: product.name,
    sku: product.sku,
    barcode: product.barcode ?? '',
    category: product.category ?? 'Uncategorized',
    costPriceGbp,
    sellPriceGbp,
    grossProfitGbp,
    grossMarginPct,
    stockByWarehouse,
    reservedByWarehouse,
    totalQty,
    totalReserved,
    totalAvailable,
    reorderPointTotal: effectiveReorderPoint,
    stockStatus,
  }
}

async function readJsonResponse(response) {
  const contentType = response.headers.get('content-type') ?? ''
  const bodyText = await response.text()

  if (!response.ok) {
    throw new Error(`Inventory request failed (${response.status}).`)
  }

  if (!contentType.toLowerCase().includes('application/json')) {
    const preview = bodyText.slice(0, 80).replace(/\s+/g, ' ')
    throw new Error(`Inventory API returned non-JSON response: ${preview}`)
  }

  try {
    return JSON.parse(bodyText)
  } catch {
    const preview = bodyText.slice(0, 80).replace(/\s+/g, ' ')
    throw new Error(`Inventory API JSON parse failed: ${preview}`)
  }
}

function SummaryCard({ label, value }) {
  return (
    <article className="border border-border bg-card p-4 shadow">
      <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{label}</p>
      <p className="mt-2 text-2xl font-semibold text-foreground">{value}</p>
    </article>
  )
}

function InventoryPage() {
  const [products, setProducts] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const [currencies, setCurrencies] = useState(FALLBACK_CURRENCIES)
  const [selectedCurrency, setSelectedCurrency] = useState('GBP')
  const [warehouseFilter, setWarehouseFilter] = useState('all')
  const [statusFilter, setStatusFilter] = useState('all')
  const [searchQuery, setSearchQuery] = useState('')

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
          throw new Error('Inventory data format is invalid.')
        }

        if (!isSubscribed) {
          return
        }

        setProducts(data.map(normalizeProduct))
      } catch (fetchError) {
        if (!isSubscribed) {
          return
        }

        const errorMessage =
          fetchError instanceof Error ? fetchError.message : 'Failed to load inventory.'
        setError(errorMessage)
        setProducts([])
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

  useEffect(() => {
    let isSubscribed = true

    async function fetchRates() {
      try {
        const response = await fetch(
          'https://api.frankfurter.dev/v1/latest?base=GBP&symbols=USD,EUR',
        )
        if (!response.ok) return
        const data = await response.json()
        if (!isSubscribed || !data.rates) return

        setCurrencies({
          GBP: { code: 'GBP', rate: 1, fxText: '1 GBP = 1.00 GBP' },
          USD: {
            code: 'USD',
            rate: data.rates.USD,
            fxText: `1 GBP = ${data.rates.USD.toFixed(4)} USD (live ${data.date})`,
          },
          EUR: {
            code: 'EUR',
            rate: data.rates.EUR,
            fxText: `1 GBP = ${data.rates.EUR.toFixed(4)} EUR (live ${data.date})`,
          },
        })
      } catch {
        // Keep fallback rates
      }
    }

    fetchRates()

    return () => {
      isSubscribed = false
    }
  }, [])

  const query = searchQuery.trim().toLowerCase()

  const filteredProducts = useMemo(() => {
    return products.filter((product) => {
      const matchesSearch =
        query.length === 0 ||
        product.name.toLowerCase().includes(query) ||
        product.barcode.toLowerCase().includes(query)

      const matchesWarehouse =
        warehouseFilter === 'all' || product.stockByWarehouse[warehouseFilter] > 0

      const matchesStatus =
        statusFilter === 'all' || product.stockStatus === statusFilter

      return matchesSearch && matchesWarehouse && matchesStatus
    })
  }, [products, query, warehouseFilter, statusFilter])

  const summary = useMemo(() => {
    const totals = filteredProducts.reduce(
      (accumulator, product) => {
        accumulator.units += product.totalQty
        accumulator.marginSum += product.grossMarginPct
        if (product.stockStatus === 'out') {
          accumulator.outOfStockCount += 1
        }
        return accumulator
      },
      { units: 0, marginSum: 0, outOfStockCount: 0 },
    )

    return {
      skuCount: filteredProducts.length,
      totalUnits: totals.units,
      averageMargin:
        filteredProducts.length > 0 ? totals.marginSum / filteredProducts.length : 0,
      outOfStockCount: totals.outOfStockCount,
    }
  }, [filteredProducts])

  const warehouseMargins = useMemo(() => {
    const warehouses = {
      uk: { totalQty: 0, marginWeightedSum: 0 },
      us: { totalQty: 0, marginWeightedSum: 0 },
    }

    for (const product of filteredProducts) {
      for (const wh of ['uk', 'us']) {
        const qty = product.stockByWarehouse[wh]
        if (qty > 0) {
          warehouses[wh].totalQty += qty
          warehouses[wh].marginWeightedSum += product.grossMarginPct * qty
        }
      }
    }

    return {
      uk:
        warehouses.uk.totalQty > 0
          ? warehouses.uk.marginWeightedSum / warehouses.uk.totalQty
          : 0,
      us:
        warehouses.us.totalQty > 0
          ? warehouses.us.marginWeightedSum / warehouses.us.totalQty
          : 0,
    }
  }, [filteredProducts])

  const globeMarkers = useMemo(
    () => [
      {
        id: 'bar-1',
        location: [51.51, -0.13],
        value: Math.round(warehouseMargins.uk),
        label: 'UK',
      },
      {
        id: 'bar-2',
        location: [40.71, -74.01],
        value: Math.round(warehouseMargins.us),
        label: 'US',
      },
    ],
    [warehouseMargins],
  )

  const fxDrift = useMemo(() => {
    const liveRate = currencies.USD.rate
    const drift = ((liveRate - CACHED_USD_RATE) / CACHED_USD_RATE) * 100
    return { liveRate, cachedRate: CACHED_USD_RATE, driftPct: drift }
  }, [currencies])

  const currency = currencies[selectedCurrency]

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

  const usdFormatter = useMemo(
    () =>
      new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }),
    [],
  )

  const selectedCurrencyFormatter = useMemo(
    () =>
      new Intl.NumberFormat('en-GB', {
        style: 'currency',
        currency: currency.code,
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }),
    [currency.code],
  )

  return (
    <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
      <section className="border border-border bg-sidebar p-6 shadow md:p-8">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
              Telemachus WMS
            </p>
            <h1 className="mt-2 text-3xl font-semibold text-sidebar-primary md:text-4xl">Inventory</h1>
            <p className="mt-2 text-sm text-muted-foreground">{currency.fxText}</p>
          </div>

          <div className="border border-sidebar-border bg-sidebar-accent p-1">
            {Object.keys(currencies).map((currencyCode) => {
              const isActive = currencyCode === selectedCurrency

              return (
                <button
                  key={currencyCode}
                  type="button"
                  onClick={() => setSelectedCurrency(currencyCode)}
                  className={`px-4 py-2 text-sm font-medium transition ${
                    isActive
                      ? 'bg-primary text-primary-foreground'
                      : 'text-sidebar-foreground hover:bg-muted'
                  }`}
                >
                  {currencyCode}
                </button>
              )
            })}
          </div>
        </div>
      </section>

      <section
        className={`mt-4 flex items-center border px-4 py-3 font-mono text-sm shadow ${
          Math.abs(fxDrift.driftPct) > 3
            ? 'border-destructive/40 bg-destructive/5'
            : 'border-border bg-card'
        }`}
      >
        <div className="flex flex-wrap items-center gap-x-6 gap-y-1">
          <span className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            GBP → USD
          </span>
          <span className="text-foreground">
            Current: <strong>{fxDrift.liveRate.toFixed(4)}</strong>
          </span>
          <span className="text-foreground">
            Cached: <strong>{fxDrift.cachedRate.toFixed(4)}</strong>
          </span>
          <span
            className={`font-semibold ${
              Math.abs(fxDrift.driftPct) > 3 ? 'text-destructive' : 'text-muted-foreground'
            }`}
          >
            Drift: {fxDrift.driftPct > 0 ? '+' : ''}
            {fxDrift.driftPct.toFixed(1)}%
            {Math.abs(fxDrift.driftPct) > 3 ? ' ⚠' : ''}
          </span>
        </div>
        <span className="group relative ml-auto cursor-help pl-4">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="h-4 w-4 text-muted-foreground"
          >
            <circle cx="12" cy="12" r="10" />
            <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
            <line x1="12" y1="17" x2="12.01" y2="17" />
          </svg>
          <span className="pointer-events-none absolute bottom-full right-0 z-50 mb-2 hidden w-72 border border-border bg-popover p-3 text-xs font-sans leading-relaxed text-popover-foreground shadow-lg backdrop-blur-sm group-hover:block">
            <strong className="block mb-1">FX Rate Drift</strong>
            Products are priced in GBP. US customers see a live USD conversion. "Cached" is the rate
            from the last catalogue export (1.3396). If the live rate has drifted more than 3%, USD
            prices shown to customers differ significantly from what they last saw — affecting
            competitiveness. The banner turns red when drift exceeds 3%.
          </span>
        </span>
      </section>

      <section className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <SummaryCard label="Total SKUs" value={summary.skuCount.toLocaleString('en-GB')} />
        <SummaryCard
          label="Total Units"
          value={summary.totalUnits.toLocaleString('en-GB')}
        />
        <SummaryCard
          label="Average Gross Margin"
          value={`${summary.averageMargin.toFixed(1)}%`}
        />
        <article className="border border-border bg-card p-4 shadow">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            Margin by Location
          </p>
          <div className="mx-auto mt-2 w-full max-w-[180px]">
            <GlobeBars markers={globeMarkers} speed={0.003} />
          </div>
        </article>
      </section>

      <section className="mt-6 border border-border bg-card p-4 shadow md:p-6">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
            Search name or barcode
            <input
              type="search"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
              placeholder="Search products"
              className="border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
            />
          </label>

          <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
            Warehouse
            <select
              value={warehouseFilter}
              onChange={(event) => setWarehouseFilter(event.target.value)}
              className="border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
            >
              <option value="all">All warehouses</option>
              {WAREHOUSES.map((warehouse) => (
                <option key={warehouse.key} value={warehouse.key}>
                  {warehouse.label}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
            Stock status
            <select
              value={statusFilter}
              onChange={(event) => setStatusFilter(event.target.value)}
              className="border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
            >
              <option value="all">All statuses</option>
              <option value="healthy">Healthy</option>
              <option value="low">Low</option>
              <option value="out">Out</option>
            </select>
          </label>

          <div className="flex items-end">
            <button
              type="button"
              onClick={() => {
                setSearchQuery('')
                setWarehouseFilter('all')
                setStatusFilter('all')
              }}
              className="w-full border border-border bg-secondary px-3 py-2 text-sm font-medium text-secondary-foreground transition hover:bg-accent hover:text-accent-foreground"
            >
              Clear filters
            </button>
          </div>
        </div>
      </section>

      <section className="mt-6 border border-border bg-card shadow">
        {loading ? (
          <div className="p-6 text-sm text-muted-foreground">Loading inventory data...</div>
        ) : error ? (
          <div className="p-6 text-sm text-destructive">Inventory data error: {error}</div>
        ) : filteredProducts.length === 0 ? (
          <div className="p-6 text-sm text-muted-foreground">No products match your filters.</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-border">
              <thead className="bg-muted text-left text-xs uppercase tracking-wide text-muted-foreground">
                <tr>
                  <th className="px-4 py-3">Product Name</th>
                  <th className="px-4 py-3">SKU / Barcode</th>
                  <th className="px-4 py-3">Category</th>
                  <th className="px-4 py-3">Cost Price (GBP)</th>
                  <th className="px-4 py-3">Sell Price ({currency.code})</th>
                  <th className="px-4 py-3">Gross Profit ({currency.code})</th>
                  <th className="px-4 py-3">Gross Margin %</th>
                  <th className="px-4 py-3">Available</th>
                  <th className="px-4 py-3">Committed</th>
                  <th className="px-4 py-3">Stock Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border text-sm text-foreground">
                {filteredProducts.map((product) => {
                  const convertedSellPrice = product.sellPriceGbp * currency.rate
                  const convertedGrossProfit = product.grossProfitGbp * currency.rate

                  return (
                    <tr key={product.id} className="hover:bg-accent">
                      <td className="px-4 py-3 font-medium text-foreground">{product.name}</td>
                      <td className="px-4 py-3">
                        <p className="font-mono text-xs text-foreground">{product.sku}</p>
                        <p className="text-xs text-muted-foreground">{product.barcode || 'No barcode'}</p>
                      </td>
                      <td className="px-4 py-3">{product.category}</td>
                      <td className="px-4 py-3">{gbpFormatter.format(product.costPriceGbp)}</td>
                      <td className="px-4 py-3">
                        {selectedCurrencyFormatter.format(convertedSellPrice)}
                      </td>
                      <td className="px-4 py-3">
                        {selectedCurrencyFormatter.format(convertedGrossProfit)}
                      </td>
                      <td className="px-4 py-3">{product.grossMarginPct.toFixed(1)}%</td>
                      <td className="px-4 py-3 font-mono text-xs">{product.totalAvailable}</td>
                      <td className={`px-4 py-3 font-mono text-xs ${product.totalReserved > 0 ? 'font-semibold text-accent-foreground' : 'text-muted-foreground'}`}>
                        {product.totalReserved}
                      </td>
                      <td className="px-4 py-3">
                        <span
                          className={`inline-flex px-2.5 py-1 text-xs font-semibold ${STATUS_CLASSES[product.stockStatus]}`}
                        >
                          {STATUS_LABELS[product.stockStatus]}
                        </span>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  )
}

export default InventoryPage
