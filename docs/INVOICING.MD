# Invoicing System

## Overview

The Invoicing page is the command center for order processing. It transforms customer orders from three different input formats (Excel files, unstructured text, or manual catalogue selection) into structured invoice data that can be saved, printed, and analyzed.

## Purpose

In traditional B2B toy distribution, the workflow looks like this:
1. Customer emails an Excel order form
2. Someone manually checks stock availability
3. Someone manually creates an invoice
4. Someone manually updates inventory records

**This is a human bottleneck at every step.**

The Invoicing system addresses this by:
- **Automating order parsing**: Extract line items from Excel files automatically
- **AI-powered structuring**: Parse unstructured emails or text into invoice lines
- **Real-time stock checking**: Automatically allocate available stock and flag backorders
- **Instant invoice generation**: Print or export invoices in seconds
- **Data capture for analytics**: Feed the Stretch intelligence layer for customer insights

## Three Input Methods

### 1. Excel File Upload

**Use case**: Customer sends a completed order form (`.xlsx` or `.xls`)

**How it works**:
1. Upload the file
2. System scans first 120 rows to find the header row
3. Identifies required columns by alias matching:
   - Product Code: `"product code"`, `"sku"`
   - Description: `"description"`, `"title"`, `"product name"`
   - Barcode: `"barcode"`, `"ean"`
   - Trade Price: `"trade price uk"`, `"trade price"`, `"trade price ex vat £"`, `"trade price ex vat"`
   - Trade Quantity: `"trade quantity"`, `"trade qty"`, `"quantity"`, `"qty"`, `"order qty"`, `"order quantity"`
4. Extracts invoice line items where quantity > 0
5. Attempts to extract metadata from cells above the header:
   - Customer name
   - Order number (or infers from filename)
   - Order date
   - Invoice date (defaults to today)

**Expected file format**:
```
Customer: Acme Toys Ltd
Order Number: ORD-2024-123
Date: 2024-03-15

| Product Code | Description           | Barcode       | Trade Price UK | Trade Quantity |
|--------------|-----------------------|---------------|----------------|----------------|
| TOY-001      | Wooden Building Block | 5060123456789 | 12.50          | 20             |
| TOY-002      | Plush Bear Large      | 5060987654321 | 18.00          | 15             |
```

**Error handling**:
- If no header row found ’ "Could not find a valid order header row..."
- If required columns missing ’ "Could not find required columns..."
- If Trade Quantity column missing ’ "This file has product pricing but no Trade Quantity column. Please upload a customer order form (not the stock list)."

### 2. Unstructured Text Parsing (AI)

**Use case**: Customer sends an email, WhatsApp message, or informal order list

**How it works**:
1. Paste raw text into the textarea
2. Click "Parse with AI"
3. System sends text to `/api/parse-unstructured`
4. LLM extracts:
   - Product names/SKUs
   - Quantities
   - Prices (if mentioned)
   - Customer name, order number, dates (if mentioned)
5. Returns structured invoice lines

**Example input**:
```
Hi, we'd like to order the following for Acme Toys:

- 20x Wooden Building Blocks (TOY-001) @ £12.50 each
- 15x Plush Bear Large (TOY-002) @ £18 each

Please invoice to order number ORD-2024-123
```

**Expected output**: Structured invoice lines with SKUs, descriptions, quantities, and prices

**Error states**:
- Network error ’ "AI parsing failed (500)"
- No lines extracted ’ "No invoice lines could be extracted from the text."

### 3. Manual Catalogue Selection

**Use case**: Phone order, in-person order, or creating a quote

**How it works**:
1. System fetches product catalogue from `/api/inventory`
2. Search by product name or SKU
3. Enter quantity for each product
4. Click "Add to Invoice"
5. System creates invoice lines using current sell prices

**Data displayed**:
- Product name
- SKU
- Current sell price (GBP)
- Available stock (qty on hand - qty reserved)
- Quantity input field

**Benefits**:
- Real-time stock visibility while taking order
- No need to hunt through Excel files
- Instant pricing from live data

## Metadata Editing

Once an order is loaded, you can edit:

| Field | Description | Default |
|-------|-------------|---------|
| **Customer Name** | Company name | Extracted from file/text, or blank |
| **Order Number** | Customer's PO number | Extracted or inferred from filename |
| **Order Date** | When customer placed order | Extracted or blank |
| **Invoice Date** | When invoice is issued | Today's date (ISO format) |
| **Customer Region** | UK / US / EU | UK |

Customer Region is critical for:
- Warehouse allocation logic
- Shipping cost calculations
- Tax/VAT handling (future)

## Invoice Actions

### Export HTML
Downloads a standalone HTML file with:
- Invoice header (customer, order number, dates)
- Summary cards (line count, total units, subtotal)
- Full line item table
- Generated timestamp footer

**Filename**: `invoice-{orderNumber}.html`

### Print / Save PDF
Opens invoice in new window and triggers print dialog. Users can:
- Print physical copy
- Save as PDF (via print ’ Save as PDF)

**Use case**: Send invoice to customer immediately

### Save to Stretch
Posts invoice data to `/api/invoices` for permanent storage and analytics. This triggers:

**Stock allocation**:
- For each line item, check available stock
- Allocate as much as possible
- Create backorder records for shortfalls
- Return fulfilment summary

**Customer tracking**:
- Link invoice to customer record (by name/region)
- Update customer total spend
- Track order history for loyalty ranking

**Product analytics**:
- Update sell-through velocity
- Identify best sellers vs slow movers
- Track revenue per SKU

**Response codes**:
- `200 OK` ’ Invoice saved successfully
- `409 Conflict` ’ Duplicate order number (already saved)
- `4xx/5xx` ’ Save failed (network or server error)

**Fulfilment Summary** shows:
```javascript
{
  fulfilled: 12,      // Lines fully allocated from stock
  partial: 3,         // Lines with some backorders
  backordered: 2      // Lines completely out of stock
}
```

**Button states**:
- `idle` ’ "Save to Stretch" (blue)
- `saving` ’ "Saving..." (disabled)
- `saved` ’ "Saved to Stretch" (grey, disabled)
- `duplicate` ’ "Already saved" (yellow)
- `error` ’ "Save failed  retry" (red)

## Technical Implementation

### File Parsing Logic

**Header detection** (`findHeaderRowIndex`):
- Scans first 120 rows
- Tries to find columns matching all required aliases
- Returns index of first valid header row
- Throws error if not found

**Column mapping** (`getColumnIndexes`):
- Normalizes headers (trim, lowercase, collapse whitespace)
- Builds Map of `normalized_header ’ column_index`
- Searches for each required column using alias list
- Supports flexible header names (e.g., "Trade Price UK" vs "Trade Price Ex VAT £")

**Date parsing** (`normalizeDateInput`):
- Handles Excel serial date codes (numeric dates)
- Parses ISO strings (`YYYY-MM-DD`)
- Parses human-readable dates via `new Date()`
- Falls back to workbook creation date if no explicit date found

**Metadata extraction** (`extractMetadata`):
- Scans all rows BEFORE header row
- Looks for inline patterns like `"Customer: Acme Toys"`
- Looks for label-value pairs like `[Customer] [Acme Toys]` in adjacent cells
- Infers order number from filename if not found in file

### Data Normalization

**Money parsing** (`toMoney`):
- Strips non-numeric characters except `.` and `-`
- Converts to number
- Returns 0 for invalid values

**Quantity parsing** (`toQuantity`):
- Trims whitespace
- Converts to number
- Returns 0 for invalid or empty values

**Why this matters**: Excel files can contain:
- Currency symbols: `£12.50` ’ `12.50`
- Thousand separators: `1,234` ’ `1234`
- Text garbage: `"12.50 each"` ’ `12.50`

### HTML Invoice Generation

The `buildInvoiceHtml` function creates a complete, self-contained HTML document with:
- Inline CSS (no external dependencies)
- Responsive table layout
- Print-optimized styling
- XSS protection via `escapeHtml` (prevents malicious content in product descriptions)

**Print styling considerations**:
- Monochrome-friendly (borders instead of colored backgrounds)
- Page break hints for multi-page invoices
- Readable font sizes for physical documents

### Frontend State Management

**Key state**:
- `invoiceLines`: Array of line items (the source of truth)
- `metadata`: Order header info
- `fileName`: Tracks which input method was used
- `saveStatus`: Tracks save lifecycle
- `fulfilmentSummary`: Shows stock allocation results
- `catalogueProducts`: Live product data for manual orders
- `catalogueQty`: Map of `productId ’ quantity` for cart behavior

**Computed state** (`useMemo`):
- `summary`: Aggregates line count, total units, total value
- `filteredCatalogue`: Real-time search filtering
- `gbpFormatter`: Intl.NumberFormat instance (reused for performance)

## Workflow Examples

### Scenario 1: Standard Excel Order
1. Customer emails `OrderForm-12345.xlsx`
2. Upload file ’ System extracts 15 line items
3. Metadata auto-filled: Order Number = `12345`, Customer = `"Acme Toys Ltd"`
4. Edit Invoice Date if needed
5. Click "Save to Stretch"
6. Result: 10 lines fulfilled, 5 partial backorders
7. Click "Print / Save PDF" ’ Send to customer

### Scenario 2: Email Order
1. Customer emails text: _"Need 20 wooden blocks and 10 plush bears"_
2. Copy/paste into unstructured text area
3. Click "Parse with AI"
4. System returns 2 line items with SKUs matched from catalogue
5. Manually add customer name and order number
6. Save to Stretch ’ Analytics layer learns customer preference for wooden toys

### Scenario 3: Phone Order
1. Customer calls and says _"I want 5 of that new puzzle set"_
2. Search catalogue: "puzzle"
3. Find product, enter quantity 5
4. Add to invoice
5. Tell customer total price and availability
6. If they confirm, save to Stretch
7. Print and mail invoice

## Integration Points

### `/api/inventory`
- **Method**: GET
- **Purpose**: Fetch live product catalogue for manual orders
- **Returns**: Array of products with `id`, `name`, `sku`, `barcode`, `sell_price_gbp`, `warehouse_stock`
- **Frontend calculation**: `available = sum(qty_on_hand) - sum(qty_reserved)` across all warehouses

### `/api/parse-unstructured`
- **Method**: POST
- **Body**: `{ text: string }`
- **Purpose**: LLM-powered extraction of invoice lines from freeform text
- **Returns**: `{ lines: [...], metadata: {...} }`
- **Expected LLM behavior**: Match product names to known SKUs, extract quantities and prices

### `/api/invoices`
- **Method**: POST
- **Body**:
```json
{
  "customerName": "Acme Toys Ltd",
  "customerRegion": "uk",
  "orderNumber": "ORD-2024-123",
  "orderDate": "2024-03-15",
  "invoiceDate": "2024-03-20",
  "sourceFilename": "OrderForm-12345.xlsx",
  "lines": [
    {
      "sku": "TOY-001",
      "description": "Wooden Building Block",
      "barcode": "5060123456789",
      "tradePrice": 12.50,
      "quantity": 20,
      "subtotal": 250.00
    }
  ]
}
```
- **Purpose**: Save invoice to database and trigger stock allocation
- **Returns**:
```json
{
  "invoiceId": 1234,
  "fulfilmentSummary": {
    "fulfilled": 12,
    "partial": 3,
    "backordered": 2
  }
}
```

## Common Errors

| Error Message | Cause | Solution |
|---------------|-------|----------|
| "Could not find a valid order header row..." | Excel file has no recognizable column headers | Ensure file has "Product Code", "Description", "Barcode", "Trade Price", "Trade Quantity" headers |
| "This file has product pricing but no Trade Quantity column" | Uploaded stock list instead of order form | Upload the customer's ORDER FORM, not your internal stock list |
| "No invoice lines found. This usually means Trade Quantity is empty for all rows." | All quantity cells are blank or zero | Customer didn't fill in quantities  ask them to complete the form |
| "AI parsing failed (500)" | Backend LLM error or timeout | Check `/api/parse-unstructured` endpoint logs |
| "No invoice lines could be extracted from the text." | Text too vague or doesn't contain recognizable products | Provide more structured text with SKUs or full product names |
| "Pop-up blocked. Please allow pop-ups to print or save as PDF." | Browser blocked print window | Enable pop-ups for this domain |

## Future Enhancements

Potential improvements:
- **Customer autocomplete**: Dropdown of known customers as you type
- **SKU autocomplete**: Suggest SKUs based on partial input in unstructured parser
- **Line item editing**: Add/remove/edit individual lines after parsing
- **Multi-currency support**: Handle USD, EUR invoices with exchange rates
- **VAT calculation**: Add tax based on customer region
- **Bulk upload**: Process multiple order files in one batch
- **Email integration**: Fetch orders directly from inbox via IMAP
- **Shopify sync**: Pull B2B orders from e-commerce platform API
- **Invoice templates**: Customizable HTML/PDF templates with company branding
- **Payment tracking**: Link invoices to payment records
