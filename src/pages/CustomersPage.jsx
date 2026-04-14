import { useCallback, useEffect, useMemo, useState } from 'react'

const EMPTY_CUSTOMER_FORM = {
  companyName: '',
  contactName: '',
  email: '',
  billingAddress: '',
  currencyPreference: 'GBP',
  vatNumber: '',
  notes: '',
}

async function readJsonResponse(response, fallbackError) {
  const bodyText = await response.text()
  const contentType = response.headers.get('content-type') ?? ''

  if (!response.ok) {
    if (bodyText) {
      throw new Error(bodyText)
    }
    throw new Error(fallbackError)
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

function toMoneyNumber(value) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function toDateText(value) {
  if (!value) {
    return '-'
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return '-'
  }

  return date.toLocaleDateString('en-GB')
}

function CustomersPage() {
  const [customers, setCustomers] = useState([])
  const [searchQuery, setSearchQuery] = useState('')
  const [loadingCustomers, setLoadingCustomers] = useState(true)
  const [customersError, setCustomersError] = useState('')

  const [isNewModalOpen, setIsNewModalOpen] = useState(false)
  const [newCustomerForm, setNewCustomerForm] = useState(EMPTY_CUSTOMER_FORM)
  const [newCustomerError, setNewCustomerError] = useState('')
  const [creatingCustomer, setCreatingCustomer] = useState(false)

  const [selectedCustomerId, setSelectedCustomerId] = useState('')
  const [selectedCustomer, setSelectedCustomer] = useState(null)
  const [customerDetailError, setCustomerDetailError] = useState('')
  const [loadingCustomerDetail, setLoadingCustomerDetail] = useState(false)

  const [isEditingCustomer, setIsEditingCustomer] = useState(false)
  const [customerDraft, setCustomerDraft] = useState(null)
  const [savingCustomer, setSavingCustomer] = useState(false)

  const [activeTab, setActiveTab] = useState('orders')
  const [customerOrders, setCustomerOrders] = useState([])
  const [customerInvoices, setCustomerInvoices] = useState([])
  const [historyLoading, setHistoryLoading] = useState(false)
  const [historyError, setHistoryError] = useState('')

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

  const fetchCustomers = useCallback(async (searchTerm = '') => {
    setLoadingCustomers(true)
    setCustomersError('')

    try {
      const params = new URLSearchParams()
      if (searchTerm) {
        params.set('search', searchTerm)
      }

      const response = await fetch(`/api/customers?${params.toString()}`, { cache: 'no-store' })
      const data = await readJsonResponse(response, 'Failed to load customers.')
      if (!Array.isArray(data)) {
        throw new Error('Invalid customers payload.')
      }

      setCustomers(data)

      if (!selectedCustomerId && data.length > 0) {
        setSelectedCustomerId(data[0].id)
      }

      if (selectedCustomerId && !data.some((customer) => customer.id === selectedCustomerId)) {
        setSelectedCustomerId(data[0]?.id ?? '')
      }
    } catch (error) {
      setCustomers([])
      setCustomersError(error instanceof Error ? error.message : 'Failed to load customers.')
    } finally {
      setLoadingCustomers(false)
    }
  }, [selectedCustomerId])

  const fetchCustomerDetail = useCallback(async (customerId) => {
    if (!customerId) {
      setSelectedCustomer(null)
      setCustomerDraft(null)
      setCustomerDetailError('')
      return
    }

    setLoadingCustomerDetail(true)
    setCustomerDetailError('')

    try {
      const response = await fetch(`/api/customers/${customerId}`, { cache: 'no-store' })
      const detail = await readJsonResponse(response, 'Failed to load customer detail.')
      setSelectedCustomer(detail)
      setCustomerDraft({
        companyName: detail.company_name ?? '',
        contactName: detail.contact_name ?? '',
        email: detail.email ?? '',
        billingAddress: detail.billing_address ?? '',
        currencyPreference: detail.currency_preference ?? 'GBP',
        vatNumber: detail.vat_number ?? '',
        notes: detail.notes ?? '',
      })
    } catch (error) {
      setSelectedCustomer(null)
      setCustomerDraft(null)
      setCustomerDetailError(error instanceof Error ? error.message : 'Failed to load customer detail.')
    } finally {
      setLoadingCustomerDetail(false)
    }
  }, [])

  const fetchCustomerHistory = useCallback(async (customerId, tab) => {
    if (!customerId) {
      setCustomerOrders([])
      setCustomerInvoices([])
      setHistoryError('')
      return
    }

    setHistoryLoading(true)
    setHistoryError('')

    try {
      if (tab === 'orders') {
        const response = await fetch(`/api/customers/${customerId}/orders`, { cache: 'no-store' })
        const orders = await readJsonResponse(response, 'Failed to load order history.')
        setCustomerOrders(Array.isArray(orders) ? orders : [])
      } else {
        const response = await fetch(`/api/customers/${customerId}/invoices`, { cache: 'no-store' })
        const invoices = await readJsonResponse(response, 'Failed to load invoice history.')
        setCustomerInvoices(Array.isArray(invoices) ? invoices : [])
      }
    } catch (error) {
      setHistoryError(error instanceof Error ? error.message : 'Failed to load history.')
      if (tab === 'orders') {
        setCustomerOrders([])
      } else {
        setCustomerInvoices([])
      }
    } finally {
      setHistoryLoading(false)
    }
  }, [])

  useEffect(() => {
    const timeoutId = window.setTimeout(() => {
      fetchCustomers(searchQuery.trim())
    }, 200)

    return () => {
      window.clearTimeout(timeoutId)
    }
  }, [fetchCustomers, searchQuery])

  useEffect(() => {
    fetchCustomerDetail(selectedCustomerId)
    setIsEditingCustomer(false)
  }, [fetchCustomerDetail, selectedCustomerId])

  useEffect(() => {
    fetchCustomerHistory(selectedCustomerId, activeTab)
  }, [activeTab, fetchCustomerHistory, selectedCustomerId])

  function openNewCustomerModal() {
    setNewCustomerForm(EMPTY_CUSTOMER_FORM)
    setNewCustomerError('')
    setIsNewModalOpen(true)
  }

  function closeNewCustomerModal() {
    if (creatingCustomer) {
      return
    }
    setIsNewModalOpen(false)
  }

  function handleNewCustomerChange(field, value) {
    setNewCustomerForm((previous) => ({
      ...previous,
      [field]: value,
    }))
  }

  async function handleCreateCustomer(event) {
    event.preventDefault()
    setCreatingCustomer(true)
    setNewCustomerError('')

    try {
      const response = await fetch('/api/customers', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newCustomerForm),
      })
      const createdCustomer = await readJsonResponse(response, 'Failed to create customer.')

      setIsNewModalOpen(false)
      setSelectedCustomerId(createdCustomer.id)
      await fetchCustomers(searchQuery.trim())
      await fetchCustomerDetail(createdCustomer.id)
    } catch (error) {
      setNewCustomerError(error instanceof Error ? error.message : 'Failed to create customer.')
    } finally {
      setCreatingCustomer(false)
    }
  }

  function startEditCustomer() {
    if (!selectedCustomer || !customerDraft) {
      return
    }
    setIsEditingCustomer(true)
  }

  function cancelEditCustomer() {
    if (!selectedCustomer) {
      return
    }

    setCustomerDraft({
      companyName: selectedCustomer.company_name ?? '',
      contactName: selectedCustomer.contact_name ?? '',
      email: selectedCustomer.email ?? '',
      billingAddress: selectedCustomer.billing_address ?? '',
      currencyPreference: selectedCustomer.currency_preference ?? 'GBP',
      vatNumber: selectedCustomer.vat_number ?? '',
      notes: selectedCustomer.notes ?? '',
    })
    setIsEditingCustomer(false)
  }

  function updateCustomerDraft(field, value) {
    setCustomerDraft((previous) => ({
      ...(previous ?? EMPTY_CUSTOMER_FORM),
      [field]: value,
    }))
  }

  async function saveCustomerEdits() {
    if (!selectedCustomer || !customerDraft) {
      return
    }

    setSavingCustomer(true)
    setCustomerDetailError('')

    try {
      const response = await fetch(`/api/customers/${selectedCustomer.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(customerDraft),
      })
      await readJsonResponse(response, 'Failed to save customer details.')
      await fetchCustomers(searchQuery.trim())
      await fetchCustomerDetail(selectedCustomer.id)
      setIsEditingCustomer(false)
    } catch (error) {
      setCustomerDetailError(
        error instanceof Error ? error.message : 'Failed to save customer details.',
      )
    } finally {
      setSavingCustomer(false)
    }
  }

  return (
    <main className="mx-auto w-full max-w-7xl p-4 md:p-8">
      <section className="border border-border bg-sidebar p-6 shadow md:p-8">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">CRM</p>
        <div className="mt-2 flex flex-wrap items-end justify-between gap-4">
          <div>
            <h1 className="text-3xl font-semibold text-sidebar-primary md:text-4xl">Customers</h1>
            <p className="mt-2 text-sm text-muted-foreground">
              B2B customer records linked to orders and invoices.
            </p>
          </div>
          <button
            type="button"
            onClick={openNewCustomerModal}
            className="border border-border bg-primary px-4 py-2 text-sm font-medium text-primary-foreground transition hover:opacity-90"
          >
            New customer
          </button>
        </div>
      </section>

      <section className="mt-6 grid gap-6 lg:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)]">
        <article className="border border-border bg-card shadow">
          <div className="border-b border-border p-4 md:p-6">
            <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
              Search by company name
              <input
                type="search"
                value={searchQuery}
                onChange={(event) => setSearchQuery(event.target.value)}
                placeholder="Type to search customers"
                className="border border-input bg-background px-3 py-2 text-sm text-foreground focus:border-ring focus:outline-none focus:ring-1 focus:ring-ring"
              />
            </label>
          </div>

          {customersError ? (
            <div className="p-4 text-sm text-destructive md:p-6">{customersError}</div>
          ) : null}

          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-border">
              <thead className="bg-muted text-left text-xs uppercase tracking-wide text-muted-foreground">
                <tr>
                  <th className="px-4 py-3">Company</th>
                  <th className="px-4 py-3">Contact</th>
                  <th className="px-4 py-3">Email</th>
                  <th className="px-4 py-3">Currency</th>
                  <th className="px-4 py-3">Orders</th>
                  <th className="px-4 py-3">Total Spend</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border text-sm text-foreground">
                {loadingCustomers ? (
                  <tr>
                    <td className="px-4 py-5 text-muted-foreground" colSpan={6}>
                      Loading customers...
                    </td>
                  </tr>
                ) : customers.length === 0 ? (
                  <tr>
                    <td className="px-4 py-5 text-muted-foreground" colSpan={6}>
                      No customers found.
                    </td>
                  </tr>
                ) : (
                  customers.map((customer) => {
                    const isSelected = selectedCustomerId === customer.id

                    return (
                      <tr
                        key={customer.id}
                        className={`cursor-pointer transition hover:bg-accent ${
                          isSelected ? 'bg-accent' : ''
                        }`}
                        onClick={() => setSelectedCustomerId(customer.id)}
                      >
                        <td className="px-4 py-3 font-medium">{customer.company_name}</td>
                        <td className="px-4 py-3">{customer.contact_name || '-'}</td>
                        <td className="px-4 py-3">{customer.email || '-'}</td>
                        <td className="px-4 py-3">{customer.currency_preference || 'GBP'}</td>
                        <td className="px-4 py-3">{customer.order_count ?? 0}</td>
                        <td className="px-4 py-3">
                          {gbpFormatter.format(toMoneyNumber(customer.total_spend_gbp))}
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </article>

        <article className="border border-border bg-card p-4 shadow md:p-6">
          {loadingCustomerDetail ? (
            <p className="text-sm text-muted-foreground">Loading customer detail...</p>
          ) : !selectedCustomer ? (
            <p className="text-sm text-muted-foreground">Select a customer to view details.</p>
          ) : (
            <>
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div>
                  <h2 className="text-lg font-semibold text-foreground">Company Info</h2>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Created {toDateText(selectedCustomer.created_at)}
                  </p>
                </div>

                {!isEditingCustomer ? (
                  <button
                    type="button"
                    onClick={startEditCustomer}
                    className="border border-border bg-secondary px-3 py-2 text-xs font-semibold uppercase tracking-wide text-secondary-foreground transition hover:bg-accent"
                  >
                    Edit
                  </button>
                ) : (
                  <div className="flex gap-2">
                    <button
                      type="button"
                      onClick={cancelEditCustomer}
                      disabled={savingCustomer}
                      className="border border-border bg-secondary px-3 py-2 text-xs font-semibold uppercase tracking-wide text-secondary-foreground transition hover:bg-accent disabled:opacity-50"
                    >
                      Cancel
                    </button>
                    <button
                      type="button"
                      onClick={saveCustomerEdits}
                      disabled={savingCustomer}
                      className="border border-border bg-primary px-3 py-2 text-xs font-semibold uppercase tracking-wide text-primary-foreground transition hover:opacity-90 disabled:opacity-50"
                    >
                      {savingCustomer ? 'Saving...' : 'Save'}
                    </button>
                  </div>
                )}
              </div>

              {customerDetailError ? (
                <p className="mt-4 text-sm text-destructive">{customerDetailError}</p>
              ) : null}

              <div className="mt-4 grid gap-4 md:grid-cols-2">
                <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
                  Company Name
                  <input
                    type="text"
                    value={customerDraft?.companyName ?? ''}
                    onChange={(event) => updateCustomerDraft('companyName', event.target.value)}
                    disabled={!isEditingCustomer}
                    className="border border-input bg-background px-3 py-2 text-sm text-foreground disabled:opacity-70"
                  />
                </label>
                <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
                  Contact Name
                  <input
                    type="text"
                    value={customerDraft?.contactName ?? ''}
                    onChange={(event) => updateCustomerDraft('contactName', event.target.value)}
                    disabled={!isEditingCustomer}
                    className="border border-input bg-background px-3 py-2 text-sm text-foreground disabled:opacity-70"
                  />
                </label>
                <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
                  Email
                  <input
                    type="email"
                    value={customerDraft?.email ?? ''}
                    onChange={(event) => updateCustomerDraft('email', event.target.value)}
                    disabled={!isEditingCustomer}
                    className="border border-input bg-background px-3 py-2 text-sm text-foreground disabled:opacity-70"
                  />
                </label>
                <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
                  Currency Preference
                  <select
                    value={customerDraft?.currencyPreference ?? 'GBP'}
                    onChange={(event) => updateCustomerDraft('currencyPreference', event.target.value)}
                    disabled={!isEditingCustomer}
                    className="border border-input bg-background px-3 py-2 text-sm text-foreground disabled:opacity-70"
                  >
                    <option value="GBP">GBP</option>
                    <option value="USD">USD</option>
                    <option value="EUR">EUR</option>
                  </select>
                </label>
                <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
                  VAT Number
                  <input
                    type="text"
                    value={customerDraft?.vatNumber ?? ''}
                    onChange={(event) => updateCustomerDraft('vatNumber', event.target.value)}
                    disabled={!isEditingCustomer}
                    className="border border-input bg-background px-3 py-2 text-sm text-foreground disabled:opacity-70"
                  />
                </label>
                <label className="flex flex-col gap-2 text-sm font-medium text-foreground md:col-span-2">
                  Billing Address
                  <textarea
                    value={customerDraft?.billingAddress ?? ''}
                    onChange={(event) => updateCustomerDraft('billingAddress', event.target.value)}
                    disabled={!isEditingCustomer}
                    rows={3}
                    className="border border-input bg-background px-3 py-2 text-sm text-foreground disabled:opacity-70"
                  />
                </label>
                <label className="flex flex-col gap-2 text-sm font-medium text-foreground md:col-span-2">
                  Notes
                  <textarea
                    value={customerDraft?.notes ?? ''}
                    onChange={(event) => updateCustomerDraft('notes', event.target.value)}
                    disabled={!isEditingCustomer}
                    rows={3}
                    className="border border-input bg-background px-3 py-2 text-sm text-foreground disabled:opacity-70"
                  />
                </label>
              </div>

              <div className="mt-6 border-t border-border pt-4">
                <div className="flex gap-2">
                  <button
                    type="button"
                    onClick={() => setActiveTab('orders')}
                    className={`border px-3 py-2 text-xs font-semibold uppercase tracking-wide ${
                      activeTab === 'orders'
                        ? 'border-border bg-primary text-primary-foreground'
                        : 'border-border bg-secondary text-secondary-foreground hover:bg-accent'
                    }`}
                  >
                    Order History
                  </button>
                  <button
                    type="button"
                    onClick={() => setActiveTab('invoices')}
                    className={`border px-3 py-2 text-xs font-semibold uppercase tracking-wide ${
                      activeTab === 'invoices'
                        ? 'border-border bg-primary text-primary-foreground'
                        : 'border-border bg-secondary text-secondary-foreground hover:bg-accent'
                    }`}
                  >
                    Invoice History
                  </button>
                </div>

                {historyError ? <p className="mt-3 text-sm text-destructive">{historyError}</p> : null}

                {historyLoading ? (
                  <p className="mt-3 text-sm text-muted-foreground">Loading history...</p>
                ) : activeTab === 'orders' ? (
                  customerOrders.length === 0 ? (
                    <p className="mt-3 text-sm text-muted-foreground">
                      No orders yet. Orders will appear here after confirmation from /orders/new.
                    </p>
                  ) : (
                    <ul className="mt-3 divide-y divide-border border border-border bg-background">
                      {customerOrders.map((order) => (
                        <li key={order.id} className="flex items-center justify-between gap-3 p-3 text-sm">
                          <a
                            href={`/orders/new?orderId=${order.id}`}
                            className="font-medium text-primary hover:underline"
                          >
                            {order.order_ref}
                          </a>
                          <span className="text-muted-foreground">{order.status}</span>
                          <span className="text-muted-foreground">{toDateText(order.created_at)}</span>
                          <span>{gbpFormatter.format(toMoneyNumber(order.total_gbp))}</span>
                        </li>
                      ))}
                    </ul>
                  )
                ) : customerInvoices.length === 0 ? (
                  <p className="mt-3 text-sm text-muted-foreground">
                    No invoices yet. Invoice links will appear in Feature 06.
                  </p>
                ) : (
                  <ul className="mt-3 divide-y divide-border border border-border bg-background">
                    {customerInvoices.map((invoice) => (
                      <li key={invoice.id} className="p-3 text-sm">
                        <a href={`/invoicing?invoiceId=${invoice.id}`} className="text-primary hover:underline">
                          {invoice.invoice_ref || 'Invoice'}
                        </a>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </>
          )}
        </article>
      </section>

      {isNewModalOpen ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
          onClick={closeNewCustomerModal}
          role="presentation"
        >
          <div
            className="w-full max-w-2xl border border-border bg-card p-4 shadow md:p-6"
            onClick={(event) => event.stopPropagation()}
            role="dialog"
            aria-modal="true"
            aria-label="Create customer"
          >
            <div className="flex items-center justify-between gap-3">
              <h2 className="text-lg font-semibold text-foreground">New customer</h2>
              <button
                type="button"
                onClick={closeNewCustomerModal}
                className="border border-border bg-secondary px-3 py-1 text-xs font-semibold uppercase tracking-wide text-secondary-foreground"
              >
                Close
              </button>
            </div>

            <form className="mt-4 grid gap-4 md:grid-cols-2" onSubmit={handleCreateCustomer}>
              <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
                Company Name
                <input
                  type="text"
                  required
                  value={newCustomerForm.companyName}
                  onChange={(event) => handleNewCustomerChange('companyName', event.target.value)}
                  className="border border-input bg-background px-3 py-2 text-sm text-foreground"
                />
              </label>
              <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
                Contact Name
                <input
                  type="text"
                  value={newCustomerForm.contactName}
                  onChange={(event) => handleNewCustomerChange('contactName', event.target.value)}
                  className="border border-input bg-background px-3 py-2 text-sm text-foreground"
                />
              </label>
              <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
                Email
                <input
                  type="email"
                  value={newCustomerForm.email}
                  onChange={(event) => handleNewCustomerChange('email', event.target.value)}
                  className="border border-input bg-background px-3 py-2 text-sm text-foreground"
                />
              </label>
              <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
                Currency Preference
                <select
                  value={newCustomerForm.currencyPreference}
                  onChange={(event) =>
                    handleNewCustomerChange('currencyPreference', event.target.value)
                  }
                  className="border border-input bg-background px-3 py-2 text-sm text-foreground"
                >
                  <option value="GBP">GBP</option>
                  <option value="USD">USD</option>
                  <option value="EUR">EUR</option>
                </select>
              </label>
              <label className="flex flex-col gap-2 text-sm font-medium text-foreground">
                VAT Number
                <input
                  type="text"
                  value={newCustomerForm.vatNumber}
                  onChange={(event) => handleNewCustomerChange('vatNumber', event.target.value)}
                  className="border border-input bg-background px-3 py-2 text-sm text-foreground"
                />
              </label>
              <label className="flex flex-col gap-2 text-sm font-medium text-foreground md:col-span-2">
                Billing Address
                <textarea
                  rows={3}
                  value={newCustomerForm.billingAddress}
                  onChange={(event) => handleNewCustomerChange('billingAddress', event.target.value)}
                  className="border border-input bg-background px-3 py-2 text-sm text-foreground"
                />
              </label>
              <label className="flex flex-col gap-2 text-sm font-medium text-foreground md:col-span-2">
                Notes
                <textarea
                  rows={3}
                  value={newCustomerForm.notes}
                  onChange={(event) => handleNewCustomerChange('notes', event.target.value)}
                  className="border border-input bg-background px-3 py-2 text-sm text-foreground"
                />
              </label>

              {newCustomerError ? (
                <p className="text-sm text-destructive md:col-span-2">{newCustomerError}</p>
              ) : null}

              <div className="flex justify-end gap-3 md:col-span-2">
                <button
                  type="button"
                  onClick={closeNewCustomerModal}
                  disabled={creatingCustomer}
                  className="border border-border bg-secondary px-3 py-2 text-sm font-medium text-secondary-foreground disabled:opacity-50"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={creatingCustomer}
                  className="border border-border bg-primary px-3 py-2 text-sm font-medium text-primary-foreground disabled:opacity-50"
                >
                  {creatingCustomer ? 'Creating...' : 'Create customer'}
                </button>
              </div>
            </form>
          </div>
        </div>
      ) : null}
    </main>
  )
}

export default CustomersPage
