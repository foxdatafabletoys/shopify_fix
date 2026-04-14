import { useEffect, useMemo, useRef, useState } from 'react'
import * as XLSX from 'xlsx'

const ACCEPTED_FILE_EXTENSIONS = ['.xlsx', '.xls', '.csv']

const STATUS_STYLES = {
  fulfilled: {
    row: 'bg-emerald-50',
    label: 'Fulfilled',
    labelClass: 'bg-emerald-100 text-emerald-900',
  },
  partial: {
    row: 'bg-amber-50',
    label: 'Partial',
    labelClass: 'bg-amber-100 text-amber-900',
  },
  backordered: {
    row: 'bg-red-50',
    label: 'Backordered',
    labelClass: 'bg-red-100 text-red-900',
  },
  unmatched: {
    row: 'bg-slate-100',
    label: 'Unmatched',
    labelClass: 'bg-slate-200 text-slate-900',
  },
}

function normalizeHeader(value) {
  return String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
}

function toNumber(value) {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : 0
  }

  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed) {
      return 0
    }
    const parsed = Number(trimmed.replace(/,/g, ''))
    return Number.isFinite(parsed) ? parsed : 0
  }

  return 0
}

function normalizeLookup(value) {
  return String(value ?? '').trim().toLowerCase()
}

function findFirstHeaderIndex(headerMap, aliases) {
  for (const alias of aliases) {
    if (headerMap.has(alias)) {
      return headerMap.get(alias)
    }
  }

  return undefined
}

function getOrderColumnIndexes(headerRow) {
  const headerMap = new Map(
    headerRow.map((cellValue, index) => [normalizeHeader(cellValue), index]),
  )

  const barcodeIndex = findFirstHeaderIndex(headerMap, ['barcode', 'ean'])
  const skuIndex = findFirstHeaderIndex(headerMap, ['product code', 'sku'])
  const descriptionIndex = findFirstHeaderIndex(headerMap, ['description', 'title', 'product name'])
  const tradeQuantityIndex = findFirstHeaderIndex(headerMap, [
    'trade quantity',
    'trade qty',
    'quantity',
    'qty',
    'order quantity',
    'order qty',
  ])
  const tradePriceIndex = findFirstHeaderIndex(headerMap, [
    'trade price',
    'trade price uk',
    'trade price ex vat £',
    'trade price ex vat',
  ])
  const weightIndex = findFirstHeaderIndex(headerMap, ['weight (kg)', 'weight'])

  if (barcodeIndex === undefined || tradeQuantityIndex === undefined) {
    throw new Error('Required headers missing: Barcode/EAN and Trade Quantity are required.')
  }

  return {
    barcodeIndex,
    skuIndex,
    descriptionIndex,
    tradeQuantityIndex,
    tradePriceIndex,
    weightIndex,
  }
}

function findOrderHeaderRowIndex(rows) {
  const scanLimit = Math.min(rows.length, 120)

  for (let rowIndex = 0; rowIndex < scanLimit; rowIndex += 1) {
    const row = rows[rowIndex]
    if (!Array.isArray(row)) {
      continue
    }

    try {
      getOrderColumnIndexes(row)
      return rowIndex
    } catch {
      continue
    }
  }

  throw new Error('Could not find a valid order header row in this file.')
}

async function parseUploadedOrderFile(file) {
  const buffer = await file.arrayBuffer()
  const workbook = XLSX.read(buffer, { type: 'array' })
  const firstSheetName = workbook.SheetNames[0]

  if (!firstSheetName) {
    throw new Error('Workbook does not contain a sheet.')
  }

  const worksheet = workbook.Sheets[firstSheetName]
  const rows = XLSX.utils.sheet_to_json(worksheet, {
    header: 1,
    raw: true,
    blankrows: false,
  })

  const headerRowIndex = findOrderHeaderRowIndex(rows)
  const headerRow = rows[headerRowIndex]
  const {
    barcodeIndex,
    skuIndex,
    descriptionIndex,
    tradeQuantityIndex,
    tradePriceIndex,
    weightIndex,
  } = getOrderColumnIndexes(headerRow)

  const orderedLines = []

  for (const row of rows.slice(headerRowIndex + 1)) {
    const tradeQuantityRaw = row[tradeQuantityIndex]
    const tradeQuantity = toNumber(tradeQuantityRaw)
    const barcode = String(row[barcodeIndex] ?? '').trim()

    const tradePriceCell =
      tradePriceIndex !== undefined ? String(row[tradePriceIndex] ?? '').trim().toLowerCase() : ''

    const isTotalsRow = tradePriceCell === 'total'

    if (tradeQuantity <= 0 || !barcode || isTotalsRow) {
      continue
    }

    const sku = skuIndex !== undefined ? String(row[skuIndex] ?? '').trim() : ''
    const description =
      descriptionIndex !== undefined ? String(row[descriptionIndex] ?? '').trim() : ''
    const weightKg = weightIndex !== undefined ? toNumber(row[weightIndex]) : 0

    orderedLines.push({
      id: `${barcode}-${sku}-${description}-${orderedLines.length}`,
      barcode,
      sku,
      description,
      qtyOrdered: tradeQuantity,
      weightKg,
    })
  }

  return orderedLines
}

async function readJsonResponse(response, fallbackError) {
  const bodyText = await response.text()
  const contentType = response.headers.get('content-type') ?? ''

  if (!response.ok) {
    throw new Error(bodyText || fallbackError)
  }

  if (!contentType.toLowerCase().includes('application/json')) {
    throw new Error(fallbackError)
  }

  try {
    return JSON.parse(bodyText)
  } catch {
    throw new Error(fallbackError)
  }
}

function buildStockByWarehouse(product) {
  const stock = { uk: 0, us: 0 }

  for (const row of product.warehouse_stock ?? []) {
    if (row?.warehouse === 'uk') {
      stock.uk += toNumber(row.qty_on_hand)
    }
    if (row?.warehouse === 'us') {
      stock.us += toNumber(row.qty_on_hand)
    }
  }

  return stock
}

function assignWarehouse(product, stockByWarehouse) {
  const category = normalizeLookup(product.category)

  if (category.includes('games workshop')) {
    return 'charleston'
  }

  if (stockByWarehouse.us > stockByWarehouse.uk) {
    return 'charleston'
  }

  if (stockByWarehouse.uk > stockByWarehouse.us) {
    return 'london'
  }

  if (stockByWarehouse.us > 0 || stockByWarehouse.uk > 0) {
    return 'charleston'
  }

  return null
}

function buildMatchMaps(inventory) {
  const byBarcode = new Map()
  const bySku = new Map()

  for (const product of inventory) {
    const barcode = normalizeLookup(product.barcode)
    const sku = normalizeLookup(product.sku)

    if (barcode && !byBarcode.has(barcode)) {
      byBarcode.set(barcode, product)
    }

    if (sku && !bySku.has(sku)) {
      bySku.set(sku, product)
    }
  }

  return { byBarcode, bySku }
}

function runFulfilmentCheck(parsedLines, inventory) {
  const { byBarcode, bySku } = buildMatchMaps(inventory)

  const rows = parsedLines.map((line) => {
    const barcodeKey = normalizeLookup(line.barcode)
    const skuKey = normalizeLookup(line.sku)
    const matchedByBarcode = barcodeKey ? byBarcode.get(barcodeKey) : null
    const matchedProduct = matchedByBarcode ?? (skuKey ? bySku.get(skuKey) : null)
    const matchedBy = matchedByBarcode ? 'ean' : matchedProduct ? 'sku' : null

    if (!matchedProduct) {
      return {
        ...line,
        productId: null,
        productName: line.description || line.sku || line.barcode,
        unitPriceGbp: 0,
        totalAvailable: 0,
        qtyFulfilled: 0,
        qtyBackordered: line.qtyOrdered,
        fulfilmentStatus: 'unmatched',
        warehouseAllocated: null,
        lineTotalGbp: 0,
        matchedBy,
      }
    }

    const stockByWarehouse = buildStockByWarehouse(matchedProduct)
    const totalAvailable = stockByWarehouse.uk + stockByWarehouse.us
    const qtyFulfilled = Math.min(line.qtyOrdered, totalAvailable)
    const qtyBackordered = Math.max(0, line.qtyOrdered - qtyFulfilled)

    let fulfilmentStatus = 'backordered'
    if (totalAvailable >= line.qtyOrdered) {
      fulfilmentStatus = 'fulfilled'
    } else if (totalAvailable > 0) {
      fulfilmentStatus = 'partial'
    }

    const tradePrice = toNumber(matchedProduct.trade_price_ex_vat_gbp)
    const fallbackSellPrice = toNumber(matchedProduct.sell_price_gbp)
    const unitPriceGbp = tradePrice > 0 ? tradePrice : fallbackSellPrice

    return {
      ...line,
      productId: matchedProduct.id,
      productName: matchedProduct.name ?? line.description,
      sku: matchedProduct.sku || line.sku,
      ean: matchedProduct.barcode || line.barcode,
      category: matchedProduct.category,
      unitPriceGbp,
      totalAvailable,
      qtyFulfilled,
      qtyBackordered,
      fulfilmentStatus,
      warehouseAllocated: assignWarehouse(matchedProduct, stockByWarehouse),
      lineTotalGbp: line.qtyOrdered * unitPriceGbp,
      matchedBy,
    }
  })

  const summary = rows.reduce(
    (totals, row) => {
      totals.lineCount += 1
      totals.totalGbp += row.lineTotalGbp
      totals[row.fulfilmentStatus] += 1
      return totals
    },
    {
      lineCount: 0,
      fulfilled: 0,
      partial: 0,
      backordered: 0,
      unmatched: 0,
      totalGbp: 0,
    },
  )

  return { rows, summary }
}

function OrdersNewPage() {
  const fileInputRef = useRef(null)

  const [customers, setCustomers] = useState([])
  const [customersLoading, setCustomersLoading] = useState(true)
  const [customersError, setCustomersError] = useState('')
  const [selectedCustomerId, setSelectedCustomerId] = useState('')

  const [uploadedFileName, setUploadedFileName] = useState('')
  const [parsedLines, setParsedLines] = useState([])
  const [parsingError, setParsingError] = useState('')
  const [parsingFile, setParsingFile] = useState(false)

  const [isDragActive, setIsDragActive] = useState(false)

  const [fulfilmentRows, setFulfilmentRows] = useState([])
  const [fulfilmentSummary, setFulfilmentSummary] = useState(null)
  const [fulfilmentError, setFulfilmentError] = useState('')
  const [runningCheck, setRunningCheck] = useState(false)

  const [inventoryCache, setInventoryCache] = useState(null)

  const [confirmingOrder, setConfirmingOrder] = useState(false)
  const [confirmError, setConfirmError] = useState('')
  const [confirmResult, setConfirmResult] = useState(null)

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

  const selectedCustomer = useMemo(
    () => customers.find((customer) => customer.id === selectedCustomerId) ?? null,
    [customers, selectedCustomerId],
  )

  const summaryLine = useMemo(() => {
    if (!fulfilmentSummary) {
      return ''
    }

    return `${fulfilmentSummary.lineCount} lines · ${fulfilmentSummary.fulfilled} fulfilled · ${fulfilmentSummary.partial} partial · ${fulfilmentSummary.backordered} backordered · ${fulfilmentSummary.unmatched} unmatched · ${gbpFormatter.format(fulfilmentSummary.totalGbp)} total`
  }, [fulfilmentSummary, gbpFormatter])

  useEffect(() => {
    let isSubscribed = true

    async function fetchCustomers() {
      setCustomersLoading(true)
      setCustomersError('')

      try {
        const response = await fetch('/api/customers', { cache: 'no-store' })
        const data = await readJsonResponse(response, 'Failed to load customers.')
        if (!Array.isArray(data)) {
          throw new Error('Invalid customers payload.')
        }

        if (!isSubscribed) {
          return
        }

        setCustomers(data)
        if (data.length > 0) {
          setSelectedCustomerId((current) => current || data[0].id)
        }
      } catch (error) {
        if (!isSubscribed) {
          return
        }

        setCustomers([])
        setCustomersError(error instanceof Error ? error.message : 'Failed to load customers.')
      } finally {
        if (isSubscribed) {
          setCustomersLoading(false)
        }
      }
    }

    fetchCustomers()

    return () => {
      isSubscribed = false
    }
  }, [])

  async function handleFile(file) {
    const lowerName = file.name.toLowerCase()
    const isAllowed = ACCEPTED_FILE_EXTENSIONS.some((extension) => lowerName.endsWith(extension))
    if (!isAllowed) {
      setUploadedFileName(file.name)
      setParsedLines([])
      setParsingError('Unsupported file type. Use .xlsx, .xls, or .csv.')
      setFulfilmentRows([])
      setFulfilmentSummary(null)
      setFulfilmentError('')
      setConfirmResult(null)
      setConfirmError('')
      return
    }

    setParsingFile(true)
    setUploadedFileName(file.name)
    setParsingError('')
    setFulfilmentRows([])
    setFulfilmentSummary(null)
    setFulfilmentError('')
    setConfirmResult(null)
    setConfirmError('')

    try {
      const lines = await parseUploadedOrderFile(file)
      setParsedLines(lines)
      if (lines.length === 0) {
        setParsingError('No valid ordered lines found. Trade Quantity must be greater than 0.')
      }
    } catch (error) {
      setParsedLines([])
      setParsingError(error instanceof Error ? error.message : 'Failed to parse uploaded file.')
    } finally {
      setParsingFile(false)
    }
  }

  async function onFileInputChange(event) {
    const file = event.target.files?.[0]
    if (!file) {
      return
    }

    await handleFile(file)
    event.target.value = ''
  }

  async function runCheck() {
    if (!parsedLines.length) {
      return
    }

    setRunningCheck(true)
    setFulfilmentError('')
    setConfirmError('')
    setConfirmResult(null)

    try {
      let inventory = inventoryCache
      if (!inventory) {
        const response = await fetch('/api/inventory', { cache: 'no-store' })
        const data = await readJsonResponse(response, 'Failed to load inventory for matching.')

        if (!Array.isArray(data)) {
          throw new Error('Inventory format is invalid.')
        }

        inventory = data
        setInventoryCache(data)
      }

      const { rows, summary } = runFulfilmentCheck(parsedLines, inventory)
      setFulfilmentRows(rows)
      setFulfilmentSummary(summary)
    } catch (error) {
      setFulfilmentRows([])
      setFulfilmentSummary(null)
      setFulfilmentError(
        error instanceof Error ? error.message : 'Failed to run fulfilment check.',
      )
    } finally {
      setRunningCheck(false)
    }
  }

  async function confirmOrder() {
    if (!selectedCustomer || fulfilmentRows.length === 0) {
      return
    }

    setConfirmingOrder(true)
    setConfirmError('')
    setConfirmResult(null)

    try {
      const payload = {
        customerId: selectedCustomer.id,
        customerName: selectedCustomer.company_name,
        currency: selectedCustomer.currency_preference || 'GBP',
        lines: fulfilmentRows.map((row) => ({
          productId: row.productId,
          productName: row.productName || row.description || 'Unmatched line',
          sku: row.sku || null,
          ean: row.ean || row.barcode || null,
          qtyOrdered: row.qtyOrdered,
          qtyFulfilled: row.qtyFulfilled,
          qtyBackordered: row.qtyBackordered,
          unitPriceGbp: row.unitPriceGbp,
          warehouseAllocated: row.warehouseAllocated,
          fulfilmentStatus: row.fulfilmentStatus,
        })),
      }

      const response = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })

      const created = await readJsonResponse(response, 'Failed to confirm order.')
      setConfirmResult(created)
    } catch (error) {
      setConfirmError(error instanceof Error ? error.message : 'Failed to confirm order.')
    } finally {
      setConfirmingOrder(false)
    }
  }

  function openPicker() {
    fileInputRef.current?.click()
  }

  function onDrop(event) {
    event.preventDefault()
    setIsDragActive(false)
    const file = event.dataTransfer?.files?.[0]
    if (!file) {
      return
    }
    handleFile(file)
  }

  return (
    <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
      <section className="border border-border bg-sidebar p-6 shadow md:p-8">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
          Order Intake
        </p>
        <h1 className="mt-2 text-3xl font-semibold text-sidebar-primary md:text-4xl">New Order</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          Upload a customer order form, parse it, check stock, and confirm fulfilment.
        </p>
      </section>

      <section className="mt-6 border border-border bg-card p-4 shadow md:p-6">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">Step 1 · Upload</h2>

        <div
          role="button"
          tabIndex={0}
          onClick={openPicker}
          onKeyDown={(event) => {
            if (event.key === 'Enter' || event.key === ' ') {
              event.preventDefault()
              openPicker()
            }
          }}
          onDragEnter={(event) => {
            event.preventDefault()
            setIsDragActive(true)
          }}
          onDragOver={(event) => {
            event.preventDefault()
            setIsDragActive(true)
          }}
          onDragLeave={(event) => {
            event.preventDefault()
            setIsDragActive(false)
          }}
          onDrop={onDrop}
          className={`mt-4 border-2 border-dashed p-8 text-center transition ${
            isDragActive
              ? 'border-primary bg-accent text-foreground'
              : 'border-border bg-background text-muted-foreground hover:bg-accent'
          }`}
        >
          <p className="text-sm font-medium">
            Drag and drop order files here, or click to browse
          </p>
          <p className="mt-2 text-xs uppercase tracking-wide">
            {ACCEPTED_FILE_EXTENSIONS.join(', ')}
          </p>
        </div>

        <input
          ref={fileInputRef}
          type="file"
          accept={ACCEPTED_FILE_EXTENSIONS.join(',')}
          onChange={onFileInputChange}
          className="hidden"
        />

        {uploadedFileName ? (
          <p className="mt-3 text-xs text-muted-foreground">
            Loaded file: <span className="font-medium text-foreground">{uploadedFileName}</span>
          </p>
        ) : null}

        <div className="mt-4 grid gap-4 md:grid-cols-2">
          <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
            Customer
            <select
              value={selectedCustomerId}
              onChange={(event) => setSelectedCustomerId(event.target.value)}
              disabled={customersLoading || customers.length === 0}
              className="border border-input bg-background px-3 py-2 text-sm text-foreground"
            >
              <option value="">Select customer</option>
              {customers.map((customer) => (
                <option key={customer.id} value={customer.id}>
                  {customer.company_name} ({customer.currency_preference || 'GBP'})
                </option>
              ))}
            </select>
          </label>
          <div className="text-xs text-muted-foreground">
            {customersLoading
              ? 'Loading customers...'
              : customersError
                ? customersError
                : selectedCustomer
                  ? `Selected customer: ${selectedCustomer.company_name}`
                  : 'Create customers at /customers if the list is empty.'}
          </div>
        </div>

        {parsingFile ? (
          <p className="mt-4 text-sm text-muted-foreground">Parsing uploaded file...</p>
        ) : null}

        {parsingError ? <p className="mt-4 text-sm text-destructive">{parsingError}</p> : null}
      </section>

      {parsedLines.length > 0 ? (
        <section className="mt-6 border border-border bg-card shadow">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border p-4 md:p-6">
            <div>
              <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
                Step 1 · Parsed Preview
              </h2>
              <p className="mt-1 text-sm text-muted-foreground">
                {parsedLines.length} lines parsed with Trade Quantity &gt; 0.
              </p>
            </div>
            <button
              type="button"
              onClick={runCheck}
              disabled={runningCheck || !selectedCustomerId}
              className="border border-border bg-primary px-4 py-2 text-sm font-medium text-primary-foreground disabled:opacity-50"
            >
              {runningCheck ? 'Checking fulfilment...' : 'Run fulfilment check'}
            </button>
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-border">
              <thead className="bg-muted text-left text-xs uppercase tracking-wide text-muted-foreground">
                <tr>
                  <th className="px-4 py-3">Barcode</th>
                  <th className="px-4 py-3">SKU</th>
                  <th className="px-4 py-3">Description</th>
                  <th className="px-4 py-3">Qty Ordered</th>
                  <th className="px-4 py-3">Weight (kg)</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border text-sm text-foreground">
                {parsedLines.map((line) => (
                  <tr key={line.id} className="hover:bg-accent">
                    <td className="px-4 py-3 font-mono text-xs">{line.barcode}</td>
                    <td className="px-4 py-3 font-mono text-xs">{line.sku || '-'}</td>
                    <td className="px-4 py-3">{line.description || '-'}</td>
                    <td className="px-4 py-3">{line.qtyOrdered}</td>
                    <td className="px-4 py-3">{line.weightKg > 0 ? line.weightKg : '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {fulfilmentError ? (
        <section className="mt-6 border border-border bg-card p-4 text-sm text-destructive shadow md:p-6">
          {fulfilmentError}
        </section>
      ) : null}

      {fulfilmentRows.length > 0 && fulfilmentSummary ? (
        <section className="mt-6 border border-border bg-card shadow">
          <div className="border-b border-border p-4 md:p-6">
            <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
              Step 5 · Fulfilment Results
            </h2>
            <p className="mt-2 text-sm font-medium text-foreground">{summaryLine}</p>
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-border">
              <thead className="bg-muted text-left text-xs uppercase tracking-wide text-muted-foreground">
                <tr>
                  <th className="px-4 py-3">Status</th>
                  <th className="px-4 py-3">Warehouse</th>
                  <th className="px-4 py-3">Description</th>
                  <th className="px-4 py-3">Barcode / EAN</th>
                  <th className="px-4 py-3">SKU</th>
                  <th className="px-4 py-3">Qty Ordered</th>
                  <th className="px-4 py-3">Qty Available</th>
                  <th className="px-4 py-3">Unit Price (GBP)</th>
                  <th className="px-4 py-3">Line Total (GBP)</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border text-sm text-foreground">
                {fulfilmentRows.map((row) => {
                  const style = STATUS_STYLES[row.fulfilmentStatus] ?? STATUS_STYLES.unmatched
                  const availabilityText =
                    row.fulfilmentStatus === 'partial'
                      ? `${row.totalAvailable} of ${row.qtyOrdered}`
                      : row.totalAvailable

                  return (
                    <tr key={row.id} className={style.row}>
                      <td className="px-4 py-3">
                        <span
                          className={`inline-flex items-center px-2 py-1 text-xs font-semibold uppercase tracking-wide ${style.labelClass}`}
                        >
                          {style.label}
                        </span>
                      </td>
                      <td className="px-4 py-3">{row.warehouseAllocated || '-'}</td>
                      <td className="px-4 py-3">{row.description || row.productName || '-'}</td>
                      <td className="px-4 py-3 font-mono text-xs">{row.barcode || row.ean || '-'}</td>
                      <td className="px-4 py-3 font-mono text-xs">{row.sku || '-'}</td>
                      <td className="px-4 py-3">{row.qtyOrdered}</td>
                      <td className="px-4 py-3">{availabilityText}</td>
                      <td className="px-4 py-3">{gbpFormatter.format(row.unitPriceGbp)}</td>
                      <td className="px-4 py-3">{gbpFormatter.format(row.lineTotalGbp)}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3 border-t border-border p-4 md:p-6">
            <p className="text-xs text-muted-foreground">
              Step 6 saves order + lines and stores warehouse allocation for Feature 05 routing.
            </p>
            <button
              type="button"
              onClick={confirmOrder}
              disabled={confirmingOrder || !selectedCustomerId}
              className="border border-border bg-primary px-4 py-2 text-sm font-medium text-primary-foreground disabled:opacity-50"
            >
              {confirmingOrder ? 'Confirming order...' : 'Confirm order'}
            </button>
          </div>
        </section>
      ) : null}

      {confirmError ? (
        <section className="mt-6 border border-border bg-card p-4 text-sm text-destructive shadow md:p-6">
          {confirmError}
        </section>
      ) : null}

      {confirmResult ? (
        <section className="mt-6 border border-border bg-card p-4 shadow md:p-6">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Order Confirmed
          </h2>
          <p className="mt-2 text-sm text-foreground">
            {confirmResult.orderRef} saved with status <span className="font-semibold">{confirmResult.status}</span>
            {' · '}
            {gbpFormatter.format(toNumber(confirmResult.totalGbp))}
          </p>
        </section>
      ) : null}
    </main>
  )
}

export default OrdersNewPage
