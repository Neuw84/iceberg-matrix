import { render, screen, fireEvent, within, waitFor } from '@testing-library/react'
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

  it('disables the version tabs and compare control in the Catalogs view', () => {
    render(<App />)
    fireEvent.click(screen.getByRole('tab', { name: 'Catalogs' }))

    // The rubric has no v2/v3 dimension; the version selector stays in the
    // header for layout consistency but is grayed out and inert.
    const v2 = screen.getByRole('tab', { name: 'V2' })
    const v3 = screen.getByRole('tab', { name: 'V3' })
    expect(v2).toBeDisabled()
    expect(v3).toBeDisabled()
    expect(v2).toHaveAttribute('aria-selected', 'false')
    expect(screen.getByRole('button', { name: 'Compare versions' })).toBeDisabled()

    // Clicking a disabled tab changes nothing: back in the engines view the
    // selection is still V2.
    fireEvent.click(v3)
    fireEvent.click(screen.getByRole('tab', { name: 'Engines' }))
    expect(screen.getByRole('tab', { name: 'V2' })).toHaveAttribute('aria-selected', 'true')
    expect(screen.getByRole('tab', { name: 'V3' })).toHaveAttribute('aria-selected', 'false')
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

  it('opens a cell popover with notes and source link but no version line', async () => {
    render(<App />)
    fireEvent.click(screen.getByRole('tab', { name: 'Catalogs' }))

    // Snowflake Horizon × Managed Offering; cells carry their notes as title.
    fireEvent.click(screen.getByTitle('Fully managed SaaS with zero catalog operations.'))

    // DetailPopover is code-split, so it mounts asynchronously.
    const dialog = await screen.findByRole('dialog', {
      name: 'Details for Managed Offering on Snowflake Horizon',
    })
    expect(
      within(dialog).getByText('Fully managed SaaS with zero catalog operations.')
    ).toBeInTheDocument()
    expect(within(dialog).getByRole('link', { name: 'Source' })).toHaveAttribute(
      'href',
      'https://docs.snowflake.com/en/user-guide/tables-iceberg'
    )
    // The synthetic "current" version is not echoed as "CURRENT · Since CURRENT";
    // the subtitle is just the catalog name.
    expect(within(dialog).getByText('Snowflake Horizon')).toBeInTheDocument()
    expect(within(dialog).queryByText(/Since/)).not.toBeInTheDocument()
  })

  it('keeps catalog filters isolated from the engines view', () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')

    // Narrow the catalogs view to the Open Source group.
    fireEvent.click(screen.getByRole('tab', { name: 'Catalogs' }))
    fireEvent.click(
      within(screen.getByRole('search')).getByRole('button', { name: 'Filter by Open Source' })
    )
    expect(within(grid()).getByText('Apache Polaris')).toBeInTheDocument()
    expect(within(grid()).queryByText('Snowflake Horizon')).not.toBeInTheDocument()

    // The engines view is unaffected by the catalog-side platform filter...
    fireEvent.click(screen.getByRole('tab', { name: 'Engines' }))
    expect(within(grid()).getByText(/PyIceberg/)).toBeInTheDocument()
    expect(within(grid()).getByText(/Athena/)).toBeInTheDocument()

    // ...and the catalog filter survives the round trip.
    fireEvent.click(screen.getByRole('tab', { name: 'Catalogs' }))
    expect(within(grid()).getByText('Apache Polaris')).toBeInTheDocument()
    expect(within(grid()).queryByText('Snowflake Horizon')).not.toBeInTheDocument()
    expect(
      within(screen.getByRole('search')).getByRole('button', { name: 'Filter by Open Source' })
    ).toHaveAttribute('aria-pressed', 'true')
  })
})

describe('Matrix filters', () => {
  it('narrows rows by feature search (debounced)', async () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')
    expect(within(grid()).getByText('Hidden Partitioning')).toBeInTheDocument()

    fireEvent.change(screen.getByRole('textbox', { name: 'Search features by name' }), {
      target: { value: 'Bloom' },
    })
    // The search box propagates to the shared filter state after a 200ms debounce.
    await waitFor(() =>
      expect(within(grid()).queryByText('Hidden Partitioning')).not.toBeInTheDocument()
    )
    expect(within(grid()).getByText('Bloom Filters & Puffin')).toBeInTheDocument()
  })

  it('narrows rows to a selected category', () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')
    expect(within(grid()).getByText('Position Deletes')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Filter by Partitioning' }))
    expect(within(grid()).queryByText('Position Deletes')).not.toBeInTheDocument()
    expect(within(grid()).getByText('Hidden Partitioning')).toBeInTheDocument()
  })

  it('narrows rows by support level', () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')
    // Every "Snowflake Horizon Catalog" v2 cell is rated, so the row drops out
    // when filtering for unknown cells; "Bloom Filters & Puffin" keeps several
    // unknown cells and stays.
    expect(within(grid()).getByText('Snowflake Horizon Catalog')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Filter by unknown support' }))
    expect(within(grid()).queryByText('Snowflake Horizon Catalog')).not.toBeInTheDocument()
    expect(within(grid()).getByText('Bloom Filters & Puffin')).toBeInTheDocument()
  })

  it('narrows catalogs by level and shows the empty state when nothing matches', async () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')
    fireEvent.click(screen.getByRole('tab', { name: 'Catalogs' }))

    // The spec-support V3 row keeps unknown cells (several catalogs haven't
    // announced v3), so the unknown filter narrows to it while the fully
    // rated rubric rows drop out.
    fireEvent.click(screen.getByRole('button', { name: 'Filter by unknown support' }))
    expect(within(grid()).getByText('Iceberg V3 Spec')).toBeInTheDocument()
    expect(within(grid()).queryByText('Managed Offering')).not.toBeInTheDocument()

    // A search that matches no feature name empties the matrix...
    fireEvent.change(screen.getByRole('textbox', { name: 'Search features by name' }), {
      target: { value: 'zzz-no-such-feature' },
    })
    await waitFor(() =>
      expect(
        screen.getByText('No compatibility data available for the current filters.')
      ).toBeInTheDocument()
    )

    // ...and clearing the search restores the narrowed matrix.
    fireEvent.change(screen.getByRole('textbox', { name: 'Search features by name' }), {
      target: { value: '' },
    })
    await waitFor(() =>
      expect(within(grid()).getByText('Iceberg V3 Spec')).toBeInTheDocument()
    )
  })

  it('reveals V3-only features on the V3 tab', () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')
    // V3-only rows are hidden under the default V2 tab.
    expect(within(grid()).queryByText('Lineage Tracking')).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole('tab', { name: 'V3' }))
    expect(within(grid()).getByText('Lineage Tracking')).toBeInTheDocument()
  })

  it('clears all filters at once', () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')
    fireEvent.click(screen.getByRole('button', { name: 'Filter by Partitioning' }))
    expect(within(grid()).queryByText('Position Deletes')).not.toBeInTheDocument()

    fireEvent.click(screen.getByText('✕ Clear all'))
    expect(within(grid()).getByText('Position Deletes')).toBeInTheDocument()
    // The clear control only renders while a filter is active.
    expect(screen.queryByText('✕ Clear all')).not.toBeInTheDocument()
  })
})
