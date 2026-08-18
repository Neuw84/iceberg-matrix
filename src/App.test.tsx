import { render, screen, fireEvent } from '@testing-library/react'
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

    expect(screen.getByText('Proprietary')).toBeInTheDocument()
    expect(screen.getByText('Open Source')).toBeInTheDocument()
  })
})
