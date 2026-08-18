import { render, screen, fireEvent, within } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import fc from 'fast-check'
import App from './App'

describe('App', () => {
  it('renders the heading', () => {
    render(<App />)
    expect(screen.getByText('Apache Iceberg™ Compatibility Matrix')).toBeInTheDocument()
  })

  it('fast-check is working', () => {
    fc.assert(
      fc.property(fc.integer(), (n) => {
        expect(n + 0).toBe(n)
      })
    )
  })
})

describe('Engines/Catalogs view toggle', () => {
  it('defaults to the Engines view', () => {
    render(<App />)
    // An engine column is visible; catalog columns are not.
    expect(screen.getByText(/PyIceberg/)).toBeInTheDocument()
    expect(screen.queryByText('Apache Polaris')).not.toBeInTheDocument()
    // Engines mode carries the V2/V3 version tabs.
    expect(screen.getByRole('tab', { name: 'V2' })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: 'Engines' })).toHaveAttribute('aria-selected', 'true')
  })

  it('switches to the Catalogs view and back', () => {
    render(<App />)

    fireEvent.click(screen.getByRole('tab', { name: 'Catalogs' }))
    // Catalog columns replace the engine columns.
    expect(screen.getByText('Apache Polaris')).toBeInTheDocument()
    expect(screen.getByText('Lakekeeper')).toBeInTheDocument()
    expect(screen.queryByText(/PyIceberg/)).not.toBeInTheDocument()
    // Rubric rows are shown.
    expect(screen.getByText('Managed Offering')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('tab', { name: 'Engines' }))
    expect(screen.getByText(/PyIceberg/)).toBeInTheDocument()
    expect(screen.queryByText('Apache Polaris')).not.toBeInTheDocument()
  })

  it('shows no version tabs or compare control in the Catalogs view', () => {
    render(<App />)
    fireEvent.click(screen.getByRole('tab', { name: 'Catalogs' }))

    // The rubric has no v2/v3 dimension, so the version selector disappears
    // entirely (the view toggle itself is the only tablist left).
    expect(screen.queryByRole('tab', { name: 'V2' })).not.toBeInTheDocument()
    expect(screen.queryByRole('tab', { name: 'V3' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Compare versions' })).not.toBeInTheDocument()
  })

  it('shows both catalog groups as column group headers', () => {
    render(<App />)
    fireEvent.click(screen.getByRole('tab', { name: 'Catalogs' }))

    // Scoped to the matrix grid: the same group names also appear as filter
    // chips in the panel above it.
    const grid = screen.getByRole('grid')
    expect(within(grid).getByText('Proprietary')).toBeInTheDocument()
    expect(within(grid).getByText('Open Source')).toBeInTheDocument()
  })

  it('offers the catalog groups and only the rubric category as filters', () => {
    render(<App />)
    fireEvent.click(screen.getByRole('tab', { name: 'Catalogs' }))

    const panel = screen.getByRole('search')
    // Group chips are derived from the data, so the catalog groups appear...
    expect(within(panel).getByRole('button', { name: 'Filter by Proprietary' })).toBeInTheDocument()
    expect(within(panel).getByRole('button', { name: 'Filter by Open Source' })).toBeInTheDocument()
    // ...the section heading follows the view...
    expect(within(panel).getByText('Catalogs')).toBeInTheDocument()
    // ...and category chips only offer what the dataset contains.
    expect(within(panel).getByRole('button', { name: 'Filter by Openness Rubric' })).toBeInTheDocument()
    expect(within(panel).queryByRole('button', { name: 'Filter by Partitioning' })).not.toBeInTheDocument()
  })

  it('does not offer the rubric category in the Engines view', () => {
    render(<App />)
    const panel = screen.getByRole('search')
    expect(within(panel).queryByRole('button', { name: 'Filter by Openness Rubric' })).not.toBeInTheDocument()
    expect(within(panel).getByRole('button', { name: 'Filter by Partitioning' })).toBeInTheDocument()
  })
})
