import { useEffect, useMemo, useState } from 'react'
import * as XLSX from 'xlsx'

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10)
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

function toMoney(value) {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : 0
  }

  if (typeof value === 'string') {
    const cleaned = value.replace(/[^0-9.-]/g, '')
    const parsed = Number(cleaned)
    return Number.isFinite(parsed) ? parsed : 0
  }

  return 0
}

function toQuantity(value) {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : 0
  }

  if (typeof value === 'string') {
    const cleaned = value.trim()
    if (!cleaned) {
      return 0
    }
    const parsed = Number(cleaned)
    return Number.isFinite(parsed) ? parsed : 0
  }

  return 0
}

function normalizeHeader(value) {
  return String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
}

function findFirstIndex(headerIndex, aliases) {
  for (const alias of aliases) {
    if (headerIndex.has(alias)) {
      return headerIndex.get(alias)
    }
  }

  return undefined
}

function getColumnIndexes(headerRow, options = { requireQty: true }) {
  const headerIndex = new Map(
    headerRow.map((headerCell, index) => [normalizeHeader(headerCell), index]),
  )

  const productCodeIndex = findFirstIndex(headerIndex, ['product code', 'sku'])
  const descriptionIndex = findFirstIndex(headerIndex, ['description', 'title', 'product name'])
  const barcodeIndex = findFirstIndex(headerIndex, ['barcode', 'ean'])
  const tradePriceIndex = findFirstIndex(headerIndex, [
    'trade price uk',
    'trade price',
    'trade price ex vat £',
    'trade price ex vat',
  ])
  const tradeQtyIndex = findFirstIndex(headerIndex, [
    'trade quantity',
    'trade qty',
    'quantity',
    'qty',
    'order qty',
    'order quantity',
  ])

  if (
    productCodeIndex === undefined ||
    descriptionIndex === undefined ||
    barcodeIndex === undefined ||
    tradePriceIndex === undefined
  ) {
    throw new Error(
      'Could not find required columns. Need Product Code/SKU, Description/TITLE, Barcode/EAN, and Trade Price.',
    )
  }

  if (options.requireQty && tradeQtyIndex === undefined) {
    throw new Error(
      'This file has product pricing but no Trade Quantity column. Please upload a customer order form (not the stock list).',
    )
  }

  return {
    productCodeIndex,
    descriptionIndex,
    barcodeIndex,
    tradePriceIndex,
    tradeQtyIndex,
  }
}

function findHeaderRowIndex(rows) {
  const scanLimit = Math.min(rows.length, 120)

  for (let rowIndex = 0; rowIndex < scanLimit; rowIndex += 1) {
    const row = rows[rowIndex]
    if (!Array.isArray(row)) {
      continue
    }

    try {
      getColumnIndexes(row, { requireQty: true })
      return rowIndex
    } catch {
      continue
    }
  }

  throw new Error(
    'Could not find a valid order header row with Product Code, Description, Barcode, Trade Price, and Trade Quantity.',
  )
}

function normalizeDateInput(value) {
  if (value === null || value === undefined || value === '') {
    return ''
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10)
  }

  if (typeof value === 'number') {
    const parsed = XLSX.SSF.parse_date_code(value)
    if (parsed) {
      const month = String(parsed.m).padStart(2, '0')
      const day = String(parsed.d).padStart(2, '0')
      return `${parsed.y}-${month}-${day}`
    }
  }

  const text = String(value).trim()
  if (!text) {
    return ''
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return text
  }

  const maybeDate = new Date(text)
  if (!Number.isNaN(maybeDate.getTime())) {
    return maybeDate.toISOString().slice(0, 10)
  }

  return text
}

function inferOrderNumberFromFileName(fileName) {
  const explicitOrderMatch = fileName.match(/orderform[-_\s]*([a-z0-9-]+)/i)
  if (explicitOrderMatch) {
    return explicitOrderMatch[1]
  }

  const numericMatch = fileName.match(/(\d{4,})/)
  if (numericMatch) {
    return numericMatch[1]
  }

  return ''
}

function extractMetadata(rows, headerRowIndex, fileName, workbook) {
  const metadata = {
    customerName: '',
    orderNumber: inferOrderNumberFromFileName(fileName),
    orderDate: '',
    invoiceDate: todayIsoDate(),
  }

  const workbookDate =
    normalizeDateInput(workbook?.Props?.CreatedDate) || normalizeDateInput(workbook?.Custprops?.Date)
  if (workbookDate && !metadata.orderDate) {
    metadata.orderDate = workbookDate
  }

  for (let rowIndex = 0; rowIndex < headerRowIndex; rowIndex += 1) {
    const row = rows[rowIndex]
    if (!Array.isArray(row)) {
      continue
    }

    for (let cellIndex = 0; cellIndex < row.length; cellIndex += 1) {
      const cellValue = row[cellIndex]
      if (cellValue === null || cellValue === undefined || cellValue === '') {
        continue
      }

      const text = String(cellValue).trim()
      const normalized = normalizeHeader(text)
      const rightNeighbor = row[cellIndex + 1]
      const rightNeighborText =
        rightNeighbor !== null && rightNeighbor !== undefined ? String(rightNeighbor).trim() : ''

      if (!metadata.customerName) {
        const inlineCustomer = text.match(/customer(?:\s*name)?\s*[:#-]?\s*(.+)$/i)
        if (inlineCustomer && inlineCustomer[1]) {
          metadata.customerName = inlineCustomer[1].trim()
        } else if (
          (normalized === 'customer' || normalized === 'customer name' || normalized.includes('bill to')) &&
          rightNeighborText
        ) {
          metadata.customerName = rightNeighborText
        }
      }

      if (!metadata.orderNumber) {
        const inlineOrder = text.match(/order(?:\s*number|\s*no\.?)?\s*[:#-]?\s*([a-z0-9-]+)/i)
        if (inlineOrder && inlineOrder[1]) {
          metadata.orderNumber = inlineOrder[1].trim()
        } else if (
          (normalized === 'order number' ||
            normalized === 'order no' ||
            normalized === 'order #' ||
            normalized === 'order') &&
          rightNeighborText
        ) {
          metadata.orderNumber = rightNeighborText
        }
      }

      if (!metadata.orderDate) {
        const inlineDate = text.match(/(?:order\s*)?date\s*[:#-]?\s*(.+)$/i)
        if (inlineDate && inlineDate[1]) {
          metadata.orderDate = normalizeDateInput(inlineDate[1])
        } else if ((normalized === 'date' || normalized === 'order date') && rightNeighborText) {
          metadata.orderDate = normalizeDateInput(rightNeighborText)
        }
      }
    }
  }

  return metadata
}

function parseInvoiceFromRows(rows, fileName, workbook) {
  if (!rows.length) {
    return { lines: [], metadata: extractMetadata([], 0, fileName, workbook) }
  }

  const headerRowIndex = findHeaderRowIndex(rows)
  const headerRow = rows[headerRowIndex]
  const {
    productCodeIndex,
    descriptionIndex,
    barcodeIndex,
    tradePriceIndex,
    tradeQtyIndex,
  } = getColumnIndexes(headerRow, { requireQty: true })

  const lines = []

  for (const row of rows.slice(headerRowIndex + 1)) {
    const quantity = toQuantity(row[tradeQtyIndex])
    if (quantity <= 0) {
      continue
    }

    const tradePrice = toMoney(row[tradePriceIndex])
    const productCode = String(row[productCodeIndex] ?? '').trim()
    const description = String(row[descriptionIndex] ?? '').trim()
    const barcode = String(row[barcodeIndex] ?? '').trim()

    lines.push({
      productCode,
      description,
      barcode,
      tradePrice,
      tradeQuantity: quantity,
      subTotal: tradePrice * quantity,
    })
  }

  const metadata = extractMetadata(rows, headerRowIndex, fileName, workbook)

  return { lines, metadata }
}

function SummaryCard({ label, value }) {
  return (
    <article className="border border-border bg-card p-4 shadow">
      <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{label}</p>
      <p className="mt-2 text-2xl font-semibold text-foreground">{value}</p>
    </article>
  )
}

function buildInvoiceHtml({ metadata, lines, summary, generatedAt }) {
  const rowsHtml = lines
    .map(
      (line) => `
        <tr>
          <td>${escapeHtml(line.productCode || '-')}</td>
          <td>${escapeHtml(line.description || '-')}</td>
          <td>${escapeHtml(line.barcode || '-')}</td>
          <td class="num">£${line.tradePrice.toFixed(2)}</td>
          <td class="num">${escapeHtml(line.tradeQuantity)}</td>
          <td class="num">£${line.subTotal.toFixed(2)}</td>
        </tr>
      `,
    )
    .join('')

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Invoice ${escapeHtml(metadata.orderNumber || 'Draft')}</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #111827; }
    h1 { margin: 0 0 8px; font-size: 28px; }
    .meta { margin-bottom: 20px; font-size: 14px; display: grid; grid-template-columns: 1fr 1fr; gap: 6px 16px; }
    .meta strong { color: #374151; }
    .summary { margin: 16px 0; display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
    .summary .box { border: 1px solid #d1d5db; padding: 10px; }
    .summary .label { font-size: 11px; text-transform: uppercase; color: #6b7280; margin-bottom: 4px; }
    .summary .value { font-size: 20px; font-weight: 700; }
    table { width: 100%; border-collapse: collapse; }
    th, td { border: 1px solid #d1d5db; padding: 8px; font-size: 13px; vertical-align: top; }
    th { background: #f3f4f6; text-align: left; }
    .num { text-align: right; white-space: nowrap; }
    .footer { margin-top: 16px; font-size: 12px; color: #6b7280; }
  </style>
</head>
<body>
  <h1>Invoice</h1>
  <div class="meta">
    <div><strong>Customer:</strong> ${escapeHtml(metadata.customerName || 'Not specified')}</div>
    <div><strong>Order Number:</strong> ${escapeHtml(metadata.orderNumber || 'Not specified')}</div>
    <div><strong>Order Date:</strong> ${escapeHtml(metadata.orderDate || 'Not specified')}</div>
    <div><strong>Invoice Date:</strong> ${escapeHtml(metadata.invoiceDate || 'Not specified')}</div>
  </div>

  <div class="summary">
    <div class="box">
      <div class="label">Invoice Lines</div>
      <div class="value">${escapeHtml(summary.lineCount)}</div>
    </div>
    <div class="box">
      <div class="label">Total Units</div>
      <div class="value">${escapeHtml(summary.totalUnits)}</div>
    </div>
    <div class="box">
      <div class="label">Invoice Subtotal</div>
      <div class="value">£${summary.totalValue.toFixed(2)}</div>
    </div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Product Code</th>
        <th>Description</th>
        <th>Barcode</th>
        <th class="num">Trade Price (GBP)</th>
        <th class="num">Trade Quantity</th>
        <th class="num">Sub-Total (GBP)</th>
      </tr>
    </thead>
    <tbody>${rowsHtml}</tbody>
  </table>

  <div class="footer">Generated ${escapeHtml(generatedAt)} by Telemachus WMS.</div>
</body>
</html>`
}

function InvoicingPage() {
  const [fileName, setFileName] = useState('')
  const [invoiceLines, setInvoiceLines] = useState([])
  const [error, setError] = useState('')
  const [metadata, setMetadata] = useState({
    customerName: '',
    orderNumber: '',
    orderDate: '',
    invoiceDate: todayIsoDate(),
  })
  const [customerRegion, setCustomerRegion] = useState('uk')
  const [saveStatus, setSaveStatus] = useState('idle')
  const [fulfilmentSummary, setFulfilmentSummary] = useState(null)
  const [unstructuredText, setUnstructuredText] = useState('')
  const [aiParsing, setAiParsing] = useState(false)
  const [catalogueProducts, setCatalogueProducts] = useState([])
  const [catalogueSearch, setCatalogueSearch] = useState('')
  const [catalogueQty, setCatalogueQty] = useState(new Map())

  useEffect(() => {
    let isSubscribed = true

    async function fetchCatalogue() {
      try {
        const response = await fetch(`/api/inventory?t=${Date.now()}`, { cache: 'no-store' })
        if (!response.ok) return
        const data = await response.json()
        if (!isSubscribed || !Array.isArray(data)) return
        setCatalogueProducts(
          data.map((p) => {
            const stock = (p.warehouse_stock ?? []).reduce(
              (sum, ws) => sum + (Number(ws.qty_on_hand) || 0) - (Number(ws.qty_reserved) || 0),
              0,
            )
            return {
              id: p.id,
              name: p.name,
              sku: p.sku,
              barcode: p.barcode ?? '',
              sellPriceGbp: Number(p.sell_price_gbp) || 0,
              available: stock,
            }
          }),
        )
      } catch {
        // Catalogue is optional, fail silently
      }
    }

    fetchCatalogue()
    return () => { isSubscribed = false }
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

  const summary = useMemo(() => {
    return invoiceLines.reduce(
      (accumulator, line) => {
        accumulator.lineCount += 1
        accumulator.totalUnits += line.tradeQuantity
        accumulator.totalValue += line.subTotal
        return accumulator
      },
      { lineCount: 0, totalUnits: 0, totalValue: 0 },
    )
  }, [invoiceLines])

  function updateMetadata(field, value) {
    setMetadata((previous) => ({
      ...previous,
      [field]: value,
    }))
  }

  async function handleFileUpload(event) {
    const uploadedFile = event.target.files?.[0]
    if (!uploadedFile) {
      return
    }

    setError('')
    setFileName(uploadedFile.name)
    setInvoiceLines([])

    try {
      const fileBuffer = await uploadedFile.arrayBuffer()
      const workbook = XLSX.read(fileBuffer, { type: 'array' })
      const firstSheetName = workbook.SheetNames[0]

      if (!firstSheetName) {
        throw new Error('Workbook has no sheets.')
      }

      const worksheet = workbook.Sheets[firstSheetName]
      const rows = XLSX.utils.sheet_to_json(worksheet, {
        header: 1,
        raw: true,
        blankrows: false,
      })

      const parsed = parseInvoiceFromRows(rows, uploadedFile.name, workbook)
      setMetadata((previous) => ({
        ...previous,
        ...parsed.metadata,
        invoiceDate: parsed.metadata.invoiceDate || previous.invoiceDate || todayIsoDate(),
      }))
      setInvoiceLines(parsed.lines)
    } catch (uploadError) {
      const errorMessage =
        uploadError instanceof Error ? uploadError.message : 'Failed to parse uploaded order.'
      setError(errorMessage)
      setInvoiceLines([])
    }
  }

  function handleExportHtml() {
    if (invoiceLines.length === 0) {
      return
    }

    const generatedAt = new Date().toLocaleString('en-GB')
    const html = buildInvoiceHtml({
      metadata,
      lines: invoiceLines,
      summary,
      generatedAt,
    })

    const blob = new Blob([html], { type: 'text/html;charset=utf-8' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    const safeOrderNumber = (metadata.orderNumber || 'draft').replace(/[^a-z0-9-_]/gi, '-')
    link.href = url
    link.download = `invoice-${safeOrderNumber}.html`
    document.body.append(link)
    link.click()
    link.remove()
    URL.revokeObjectURL(url)
  }

  function handlePrintInvoice() {
    if (invoiceLines.length === 0) {
      return
    }

    const generatedAt = new Date().toLocaleString('en-GB')
    const html = buildInvoiceHtml({
      metadata,
      lines: invoiceLines,
      summary,
      generatedAt,
    })

    const printWindow = window.open('', '_blank', 'noopener,noreferrer')
    if (!printWindow) {
      setError('Pop-up blocked. Please allow pop-ups to print or save as PDF.')
      return
    }

    printWindow.document.open()
    printWindow.document.write(html)
    printWindow.document.close()
    printWindow.focus()
    setTimeout(() => {
      printWindow.print()
    }, 250)
  }

  async function handleSaveToStretch() {
    if (invoiceLines.length === 0 || saveStatus === 'saving' || saveStatus === 'saved') {
      return
    }

    setSaveStatus('saving')

    try {
      const response = await fetch('/api/invoices', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          customerName: metadata.customerName,
          customerRegion,
          orderNumber: metadata.orderNumber,
          orderDate: metadata.orderDate,
          invoiceDate: metadata.invoiceDate,
          sourceFilename: fileName,
          lines: invoiceLines.map((line) => ({
            sku: line.productCode,
            description: line.description,
            barcode: line.barcode,
            tradePrice: line.tradePrice,
            quantity: line.tradeQuantity,
            subtotal: line.subTotal,
          })),
        }),
      })

      if (response.status === 409) {
        setSaveStatus('duplicate')
        return
      }

      if (!response.ok) {
        setSaveStatus('error')
        return
      }

      const result = await response.json()
      if (result.fulfilmentSummary) {
        setFulfilmentSummary(result.fulfilmentSummary)
      }
      setSaveStatus('saved')
    } catch {
      setSaveStatus('error')
    }
  }

  async function handleAiParse() {
    if (!unstructuredText.trim() || aiParsing) return

    setAiParsing(true)
    setError('')
    setSaveStatus('idle')
    setFulfilmentSummary(null)

    try {
      const response = await fetch('/api/parse-unstructured', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: unstructuredText }),
      })

      if (!response.ok) {
        const err = await response.json().catch(() => ({}))
        throw new Error(err.error || `AI parsing failed (${response.status})`)
      }

      const result = await response.json()
      if (!result.lines?.length) {
        throw new Error('No invoice lines could be extracted from the text.')
      }

      setFileName('AI-parsed')
      setInvoiceLines(result.lines)
      setMetadata((prev) => ({
        ...prev,
        customerName: result.metadata?.customerName || prev.customerName,
        orderNumber: result.metadata?.orderNumber || prev.orderNumber,
        orderDate: result.metadata?.orderDate || prev.orderDate,
      }))
    } catch (parseError) {
      setError(parseError instanceof Error ? parseError.message : 'AI parsing failed.')
      setInvoiceLines([])
    } finally {
      setAiParsing(false)
    }
  }

  const filteredCatalogue = useMemo(() => {
    const q = catalogueSearch.trim().toLowerCase()
    if (!q) return catalogueProducts
    return catalogueProducts.filter(
      (p) => p.name.toLowerCase().includes(q) || p.sku.toLowerCase().includes(q),
    )
  }, [catalogueProducts, catalogueSearch])

  function handleCatalogueQtyChange(productId, value) {
    setCatalogueQty((prev) => {
      const next = new Map(prev)
      const qty = Math.max(0, parseInt(value, 10) || 0)
      if (qty > 0) {
        next.set(productId, qty)
      } else {
        next.delete(productId)
      }
      return next
    })
  }

  function handleAddFromCatalogue() {
    const lines = []
    for (const [productId, qty] of catalogueQty) {
      if (qty <= 0) continue
      const product = catalogueProducts.find((p) => p.id === productId)
      if (!product) continue
      lines.push({
        productCode: product.sku,
        description: product.name,
        barcode: product.barcode,
        tradePrice: product.sellPriceGbp,
        tradeQuantity: qty,
        subTotal: product.sellPriceGbp * qty,
      })
    }

    if (!lines.length) return

    setFileName('Manual order')
    setInvoiceLines(lines)
    setSaveStatus('idle')
    setFulfilmentSummary(null)
    setError('')
  }

  return (
    <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
      <section className="border border-border bg-sidebar p-6 shadow md:p-8">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
          Finance
        </p>
        <h1 className="mt-2 text-3xl font-semibold text-sidebar-primary md:text-4xl">Invoicing</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          Upload a customer order form and generate invoice lines from Trade Price and Trade Quantity.
        </p>
      </section>

      <section className="mt-6 border border-border bg-card p-4 shadow md:p-6">
        <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
          Upload order file (.xlsx)
          <input
            type="file"
            accept=".xlsx,.xls"
            onChange={handleFileUpload}
            className="border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
          />
        </label>
        {fileName ? (
          <p className="mt-3 text-xs text-muted-foreground">
            Loaded file: <span className="font-medium text-foreground">{fileName}</span>
          </p>
        ) : null}

        <div className="mt-4 flex items-center gap-3">
          <div className="h-px flex-1 bg-border" />
          <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            or paste unstructured text
          </span>
          <div className="h-px flex-1 bg-border" />
        </div>

        <div className="mt-4">
          <textarea
            value={unstructuredText}
            onChange={(event) => setUnstructuredText(event.target.value)}
            placeholder="Paste an email, order list, or any unstructured text containing product names, quantities, and prices..."
            rows={6}
            className="w-full border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
          />
          <button
            type="button"
            onClick={handleAiParse}
            disabled={!unstructuredText.trim() || aiParsing}
            className="mt-2 border border-border bg-secondary px-4 py-2 text-sm font-medium text-secondary-foreground transition hover:bg-accent hover:text-accent-foreground disabled:opacity-50"
          >
            {aiParsing ? 'Parsing with AI...' : 'Parse with AI'}
          </button>
        </div>

        <div className="mt-4 flex items-center gap-3">
          <div className="h-px flex-1 bg-border" />
          <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            or build from catalogue
          </span>
          <div className="h-px flex-1 bg-border" />
        </div>

        <div className="mt-4">
          <input
            type="search"
            value={catalogueSearch}
            onChange={(event) => setCatalogueSearch(event.target.value)}
            placeholder="Search products by name or SKU..."
            className="w-full border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
          />
          {filteredCatalogue.length > 0 ? (
            <div className="mt-2 max-h-72 overflow-y-auto border border-border">
              <table className="min-w-full divide-y divide-border">
                <thead className="sticky top-0 bg-muted text-left text-xs uppercase tracking-wide text-muted-foreground">
                  <tr>
                    <th className="px-3 py-2">Product</th>
                    <th className="px-3 py-2">SKU</th>
                    <th className="px-3 py-2 text-right">Price</th>
                    <th className="px-3 py-2 text-right">Avail</th>
                    <th className="px-3 py-2 w-20">Qty</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border text-sm">
                  {filteredCatalogue.map((product) => (
                    <tr key={product.id} className="hover:bg-accent">
                      <td className="px-3 py-1.5 text-xs">{product.name}</td>
                      <td className="px-3 py-1.5 font-mono text-xs text-muted-foreground">
                        {product.sku}
                      </td>
                      <td className="px-3 py-1.5 text-right text-xs">
                        {gbpFormatter.format(product.sellPriceGbp)}
                      </td>
                      <td className={`px-3 py-1.5 text-right text-xs ${product.available <= 0 ? 'text-destructive' : ''}`}>
                        {product.available}
                      </td>
                      <td className="px-3 py-1.5">
                        <input
                          type="number"
                          min="0"
                          value={catalogueQty.get(product.id) || ''}
                          onChange={(e) => handleCatalogueQtyChange(product.id, e.target.value)}
                          className="w-16 border border-input bg-background px-2 py-1 text-xs text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
                        />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : catalogueProducts.length === 0 ? (
            <p className="mt-2 text-xs text-muted-foreground">Loading catalogue...</p>
          ) : (
            <p className="mt-2 text-xs text-muted-foreground">No products match your search.</p>
          )}
          <button
            type="button"
            onClick={handleAddFromCatalogue}
            disabled={catalogueQty.size === 0}
            className="mt-2 border border-border bg-secondary px-4 py-2 text-sm font-medium text-secondary-foreground transition hover:bg-accent hover:text-accent-foreground disabled:opacity-50"
          >
            Add to Invoice ({catalogueQty.size} product{catalogueQty.size !== 1 ? 's' : ''})
          </button>
        </div>
      </section>

      {fileName ? (
        <section className="mt-6 border border-border bg-card p-4 shadow md:p-6">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Invoice Metadata
          </h2>
          <div className="mt-4 grid gap-4 md:grid-cols-2">
            <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
              Customer Name
              <input
                type="text"
                value={metadata.customerName}
                onChange={(event) => updateMetadata('customerName', event.target.value)}
                className="border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
              />
            </label>
            <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
              Order Number
              <input
                type="text"
                value={metadata.orderNumber}
                onChange={(event) => updateMetadata('orderNumber', event.target.value)}
                className="border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
              />
            </label>
            <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
              Order Date
              <input
                type="text"
                placeholder="YYYY-MM-DD"
                value={metadata.orderDate}
                onChange={(event) => updateMetadata('orderDate', event.target.value)}
                className="border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
              />
            </label>
            <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
              Invoice Date
              <input
                type="date"
                value={metadata.invoiceDate}
                onChange={(event) => updateMetadata('invoiceDate', event.target.value)}
                className="border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
              />
            </label>
            <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
              Customer Region
              <select
                value={customerRegion}
                onChange={(event) => setCustomerRegion(event.target.value)}
                className="border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
              >
                <option value="uk">UK</option>
                <option value="us">US</option>
                <option value="eu">EU</option>
              </select>
            </label>
          </div>
        </section>
      ) : null}

      {error ? (
        <section className="mt-6 border border-border bg-card p-6 text-sm text-destructive shadow">
          Invoice parsing error: {error}
        </section>
      ) : null}

      {invoiceLines.length > 0 ? (
        <>
          <section className="mt-6 grid gap-4 md:grid-cols-3">
            <SummaryCard label="Invoice Lines" value={summary.lineCount.toLocaleString('en-GB')} />
            <SummaryCard label="Total Units" value={summary.totalUnits.toLocaleString('en-GB')} />
            <SummaryCard label="Invoice Subtotal" value={gbpFormatter.format(summary.totalValue)} />
          </section>

          <section className="mt-6 border border-border bg-card p-4 shadow md:p-6">
            <div className="flex flex-wrap gap-3">
              <button
                type="button"
                onClick={handleExportHtml}
                className="border border-border bg-secondary px-3 py-2 text-sm font-medium text-secondary-foreground transition hover:bg-accent hover:text-accent-foreground"
              >
                Export HTML
              </button>
              <button
                type="button"
                onClick={handlePrintInvoice}
                className="border border-border bg-primary px-3 py-2 text-sm font-medium text-primary-foreground transition hover:opacity-90"
              >
                Print / Save PDF
              </button>
              <button
                type="button"
                onClick={handleSaveToStretch}
                disabled={saveStatus === 'saving' || saveStatus === 'saved'}
                className={`border px-3 py-2 text-sm font-medium transition ${
                  saveStatus === 'saved'
                    ? 'border-border bg-muted text-muted-foreground'
                    : saveStatus === 'duplicate'
                      ? 'border-accent bg-accent text-accent-foreground'
                      : saveStatus === 'error'
                        ? 'border-destructive/40 bg-destructive/10 text-destructive'
                        : 'border-border bg-secondary text-secondary-foreground hover:bg-accent hover:text-accent-foreground'
                }`}
              >
                {saveStatus === 'saving'
                  ? 'Saving...'
                  : saveStatus === 'saved'
                    ? 'Saved to Stretch'
                    : saveStatus === 'duplicate'
                      ? 'Already saved'
                      : saveStatus === 'error'
                        ? 'Save failed — retry'
                        : 'Save to Stretch'}
              </button>
            </div>
            {fulfilmentSummary ? (
              <div className="mt-3 flex flex-wrap gap-3 text-xs">
                <span className="border border-border bg-muted px-2 py-1">
                  Fulfilled: <strong>{fulfilmentSummary.fulfilled}</strong>
                </span>
                {fulfilmentSummary.partial > 0 ? (
                  <span className="border border-accent bg-accent px-2 py-1 text-accent-foreground">
                    Partial: <strong>{fulfilmentSummary.partial}</strong>
                  </span>
                ) : null}
                {fulfilmentSummary.backordered > 0 ? (
                  <span className="border border-destructive/40 bg-destructive/10 px-2 py-1 text-destructive">
                    Backordered: <strong>{fulfilmentSummary.backordered}</strong>
                  </span>
                ) : null}
              </div>
            ) : null}
          </section>

          <section className="mt-6 border border-border bg-card shadow">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-border">
                <thead className="bg-muted text-left text-xs uppercase tracking-wide text-muted-foreground">
                  <tr>
                    <th className="px-4 py-3">Product Code</th>
                    <th className="px-4 py-3">Description</th>
                    <th className="px-4 py-3">Barcode</th>
                    <th className="px-4 py-3">Trade Price (GBP)</th>
                    <th className="px-4 py-3">Trade Quantity</th>
                    <th className="px-4 py-3">Sub-Total (GBP)</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border text-sm text-foreground">
                  {invoiceLines.map((line, index) => (
                    <tr
                      key={`${line.productCode}-${line.barcode}-${line.description}-${index}`}
                      className="hover:bg-accent"
                    >
                      <td className="px-4 py-3 font-mono text-xs">{line.productCode || '-'}</td>
                      <td className="px-4 py-3">{line.description || '-'}</td>
                      <td className="px-4 py-3 font-mono text-xs">{line.barcode || '-'}</td>
                      <td className="px-4 py-3">{gbpFormatter.format(line.tradePrice)}</td>
                      <td className="px-4 py-3">{line.tradeQuantity}</td>
                      <td className="px-4 py-3">{gbpFormatter.format(line.subTotal)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : fileName && !error ? (
        <section className="mt-6 border border-border bg-card p-6 text-sm text-muted-foreground shadow">
          No invoice lines found. This usually means Trade Quantity is empty for all rows.
        </section>
      ) : null}
    </main>
  )
}

export default InvoicingPage
