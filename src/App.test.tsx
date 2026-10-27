import { render, screen, fireEvent, within, waitFor } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import fc from 'fast-check'
import App from './App'

// Catalogs and Ingestion are separate chunks loaded on demand, so opening one
// shows "Loading…" until its data arrives. Wait for that, then the view is ready.
// Going back to Engines (bundled) or to an already-opened view is synchronous.
async function openView(name: 'Catalogs' | 'Ingestion') {
  fireEvent.click(screen.getByRole('tab', { name }))
  await waitFor(() => expect(screen.queryByText('Loading…')).not.toBeInTheDocument())
  expect(screen.getByRole('grid')).toBeInTheDocument()
}


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
    // An engine column is visible (both versions selected by default, so a
    // header per version); catalog columns are not.
    expect(screen.getAllByText(/PyIceberg/).length).toBeGreaterThan(0)
    expect(screen.queryByText('Apache Polaris')).not.toBeInTheDocument()
    // Engines mode carries the V2/V3 version filter chips; both selected by
    // default, with the comparison summary off (Compare enabled, not pressed).
    expect(
      screen.getByRole('button', { name: 'Show Iceberg V2 features' })
    ).toHaveAttribute('aria-pressed', 'true')
    expect(
      screen.getByRole('button', { name: 'Show Iceberg V3 features' })
    ).toHaveAttribute('aria-pressed', 'true')
    const compare = screen.getByRole('button', { name: 'Compare versions' })
    expect(compare).toBeEnabled()
    expect(compare).toHaveAttribute('aria-pressed', 'false')
    expect(screen.getByRole('tab', { name: 'Engines' })).toHaveAttribute('aria-selected', 'true')
  })

  it('switches to the Catalogs view and back', async () => {
    render(<App />)

    await openView('Catalogs')
    // Catalog columns replace the engine columns.
    expect(screen.getByText('Apache Polaris')).toBeInTheDocument()
    expect(screen.getByText('Lakekeeper')).toBeInTheDocument()
    expect(screen.queryByText(/PyIceberg/)).not.toBeInTheDocument()
    // Rubric rows are shown.
    expect(screen.getByText('Managed Offering')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('tab', { name: 'Engines' }))
    expect(screen.getAllByText(/PyIceberg/).length).toBeGreaterThan(0)
    expect(screen.queryByText('Apache Polaris')).not.toBeInTheDocument()
  })

  it('hides the version filter in the Catalogs view and keeps the engines selection', async () => {
    render(<App />)
    // Engines view offers the version chips.
    expect(
      screen.getByRole('button', { name: 'Show Iceberg V2 features' })
    ).toBeInTheDocument()

    await openView('Catalogs')
    // The rubric has no v2/v3 dimension, so the version chips are not rendered.
    expect(
      screen.queryByRole('button', { name: 'Show Iceberg V2 features' })
    ).not.toBeInTheDocument()
    expect(
      screen.queryByRole('button', { name: 'Show Iceberg V3 features' })
    ).not.toBeInTheDocument()

    // Back in the engines view the selection is still both versions (default).
    fireEvent.click(screen.getByRole('tab', { name: 'Engines' }))
    expect(
      screen.getByRole('button', { name: 'Show Iceberg V2 features' })
    ).toHaveAttribute('aria-pressed', 'true')
    expect(
      screen.getByRole('button', { name: 'Show Iceberg V3 features' })
    ).toHaveAttribute('aria-pressed', 'true')
  })

  it('shows both catalog groups as column group headers', async () => {
    render(<App />)
    await openView('Catalogs')

    // Scoped to the matrix grid: the same group names also appear as filter
    // chips in the panel above it.
    const grid = screen.getByRole('grid')
    expect(within(grid).getByText('Proprietary')).toBeInTheDocument()
    expect(within(grid).getByText('Open Source')).toBeInTheDocument()
  })

  it('offers the catalog groups and only the rubric category as filters', async () => {
    render(<App />)
    await openView('Catalogs')

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
    await openView('Catalogs')

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

  it('keeps catalog filters isolated from the engines view', async () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')

    // Narrow the catalogs view to the Open Source group.
    await openView('Catalogs')
    fireEvent.click(
      within(screen.getByRole('search')).getByRole('button', { name: 'Filter by Open Source' })
    )
    expect(within(grid()).getByText('Apache Polaris')).toBeInTheDocument()
    expect(within(grid()).queryByText('Snowflake Horizon')).not.toBeInTheDocument()

    // The engines view is unaffected by the catalog-side platform filter...
    // (both versions are selected by default, so one header per version).
    fireEvent.click(screen.getByRole('tab', { name: 'Engines' }))
    expect(within(grid()).getAllByText(/PyIceberg/).length).toBeGreaterThan(0)
    expect(within(grid()).getAllByText(/Athena/).length).toBeGreaterThan(0)

    // ...and the catalog filter survives the round trip.
    await openView('Catalogs')
    expect(within(grid()).getByText('Apache Polaris')).toBeInTheDocument()
    expect(within(grid()).queryByText('Snowflake Horizon')).not.toBeInTheDocument()
    expect(
      within(screen.getByRole('search')).getByRole('button', { name: 'Filter by Open Source' })
    ).toHaveAttribute('aria-pressed', 'true')
  })
})

describe('Ingestion view', () => {
  it('orders the tabs Engines, Ingestion, Catalogs', () => {
    render(<App />)
    const tabs = screen.getAllByRole('tab').map((t) => t.textContent)
    expect(tabs).toEqual(['Engines', 'Ingestion', 'Catalogs'])
  })

  it('shows the ingestion tools and only their write-oriented features', async () => {
    render(<App />)
    await openView('Ingestion')
    const grid = screen.getByRole('grid')

    // The ingestion tools replace the engine columns.
    expect(within(grid).getByText('Data Firehose')).toBeInTheDocument()
    expect(within(grid).getByText('Kafka Connect (1.12.0)')).toBeInTheDocument()
    expect(within(grid).getByText('OLake Go (0.12.0)')).toBeInTheDocument()
    expect(within(grid).getByText('Pixeltable (0.7.16)')).toBeInTheDocument()
    expect(within(grid).getByText('Arc (26.09.3)')).toBeInTheDocument()
    expect(within(grid).queryByText(/PyIceberg/)).not.toBeInTheDocument()

    // Write-path rows are present; read-oriented rows are not.
    expect(within(grid).getByText('Append (INSERT)')).toBeInTheDocument()
    expect(within(grid).getByText('Upsert / Delete (CDC)')).toBeInTheDocument()
    expect(within(grid).queryByText('Read Support')).not.toBeInTheDocument()
    expect(within(grid).queryByText('Time Travel / Snapshots')).not.toBeInTheDocument()
  })

  it('keeps the version chips, but not the engine-only storage toggles', async () => {
    render(<App />)
    await openView('Ingestion')
    expect(
      screen.getByRole('button', { name: 'Show Iceberg V3 features' })
    ).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^Switch to .* storage$/ })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^Switch to S3/ })).not.toBeInTheDocument()
  })

  it('does not list the ingestion tools among the engines', () => {
    render(<App />)
    const grid = screen.getByRole('grid')
    expect(within(grid).queryByText('Data Firehose')).not.toBeInTheDocument()
    expect(within(grid).queryByText(/Kafka Connect/)).not.toBeInTheDocument()
    expect(within(grid).queryByText(/OLake Go/)).not.toBeInTheDocument()
    expect(within(grid).queryByText(/Pixeltable/)).not.toBeInTheDocument()
    expect(within(grid).queryByText(/^Arc \(/)).not.toBeInTheDocument()
  })
})

describe('Matrix filters', () => {
  it('narrows rows by feature search (debounced)', async () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')
    expect(within(grid()).getByText('Hidden Partitioning')).toBeInTheDocument()

    fireEvent.change(screen.getByRole('textbox', { name: 'Search features or engines by name' }), {
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
    fireEvent.click(screen.getByRole('button', { name: 'Filter by Azure' }))
    expect(within(grid()).getByText('Snowflake Horizon Catalog')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Filter by unknown support' }))
    expect(within(grid()).queryByText('Snowflake Horizon Catalog')).not.toBeInTheDocument()
    expect(within(grid()).getByText('Bloom Filters & Puffin')).toBeInTheDocument()
  })

  it('narrows catalogs by level and shows the empty state when nothing matches', async () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')
    await openView('Catalogs')

    // The spec-support V3 row keeps unknown cells (several catalogs haven't
    // announced v3), so the unknown filter narrows to it while the fully
    // rated rubric rows drop out.
    fireEvent.click(screen.getByRole('button', { name: 'Filter by unknown support' }))
    expect(within(grid()).getByText('Iceberg V3 Spec')).toBeInTheDocument()
    expect(within(grid()).queryByText('Managed Offering')).not.toBeInTheDocument()

    // A search that matches no feature name empties the matrix...
    fireEvent.change(screen.getByRole('textbox', { name: 'Search features or catalogs by name' }), {
      target: { value: 'zzz-no-such-feature' },
    })
    await waitFor(() =>
      expect(
        screen.getByText('No compatibility data available for the current filters.')
      ).toBeInTheDocument()
    )

    // ...and clearing the search restores the narrowed matrix.
    fireEvent.change(screen.getByRole('textbox', { name: 'Search features or catalogs by name' }), {
      target: { value: '' },
    })
    await waitFor(() =>
      expect(within(grid()).getByText('Iceberg V3 Spec')).toBeInTheDocument()
    )
  })

  it('shows V3-only features by default and hides them when V3 is deselected', () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')
    // Both versions are selected by default, so V3-only rows are visible.
    expect(within(grid()).getByText('Lineage Tracking')).toBeInTheDocument()

    // Deselecting V3 (leaving only V2) hides the V3-only rows.
    fireEvent.click(screen.getByRole('button', { name: 'Show Iceberg V3 features' }))
    expect(within(grid()).queryByText('Lineage Tracking')).not.toBeInTheDocument()
  })

  it('shows write-rated position deletes only for Iceberg V2', () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')
    const row = within(grid()).getByText('Position Deletes').closest('tr')
    expect(row).not.toBeNull()
    expect(within(row!).getByLabelText('Applies to Iceberg V2')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Show Iceberg V2 features' }))
    expect(within(grid()).queryByText('Position Deletes')).not.toBeInTheDocument()
    expect(within(grid()).getByText('Deletion Vectors')).toBeInTheDocument()
  })

  it('toggles the comparison summary with the Compare button, independent of version selection', async () => {
    render(<App />)
    // Both versions selected by default, but comparison is off: no summary.
    expect(screen.queryByText('Comparison: V2 → V3')).not.toBeInTheDocument()

    // Turning Compare on shows the summary (lazy-loaded, so await it).
    fireEvent.click(screen.getByRole('button', { name: 'Compare versions' }))
    expect(await screen.findByText('Comparison: V2 → V3')).toBeInTheDocument()

    // Clicking again hides it.
    fireEvent.click(screen.getByRole('button', { name: 'Hide version comparison' }))
    await waitFor(() =>
      expect(screen.queryByText('Comparison: V2 → V3')).not.toBeInTheDocument()
    )
  })

  it('disables Compare and clears comparison when a single version is selected', async () => {
    render(<App />)
    // Turn comparison on first.
    fireEvent.click(screen.getByRole('button', { name: 'Compare versions' }))
    expect(await screen.findByText('Comparison: V2 → V3')).toBeInTheDocument()

    // Selecting a single version (deselect V3) clears comparison and disables
    // the Compare button.
    fireEvent.click(screen.getByRole('button', { name: 'Show Iceberg V3 features' }))
    await waitFor(() =>
      expect(screen.queryByText('Comparison: V2 → V3')).not.toBeInTheDocument()
    )
    expect(screen.getByRole('button', { name: 'Compare versions' })).toBeDisabled()
  })

  it('finds an engine by name: keeps that column and every feature row', async () => {
    render(<App />)
    const grid = () => screen.getByRole('grid')
    expect(within(grid()).getAllByText(/Athena/).length).toBeGreaterThan(0)

    fireEvent.change(screen.getByRole('textbox', { name: 'Search features or engines by name' }), {
      target: { value: 'duckdb' },
    })
    // Only the DuckDB column is left...
    await waitFor(() =>
      expect(within(grid()).queryByText(/Athena/)).not.toBeInTheDocument()
    )
    expect(within(grid()).getByText(/DuckDB/)).toBeInTheDocument()
    // ...with all the feature rows still shown (it is not also a feature filter).
    expect(within(grid()).getByText('Position Deletes')).toBeInTheDocument()
    expect(within(grid()).getByText('Hidden Partitioning')).toBeInTheDocument()
    // A hint says what matched.
    expect(screen.getByText(/Showing 1 engine matching “duckdb”/)).toBeInTheDocument()
  })

  it('matches an engine by vendor and keeps all of its variants', async () => {
    render(<App />)
    fireEvent.change(screen.getByRole('textbox', { name: 'Search features or engines by name' }), {
      target: { value: 'aws' },
    })
    await waitFor(() =>
      expect(screen.getByText(/Showing \d+ engines matching “aws”/)).toBeInTheDocument()
    )
    const grid = screen.getByRole('grid')
    expect(within(grid).getAllByText(/Athena/).length).toBeGreaterThan(0)
    expect(within(grid).queryByText(/DuckDB/)).not.toBeInTheDocument()
  })

  it('prefers the engine over a feature with the same word', async () => {
    render(<App />)
    // "snowflake" is both an engine and part of the "Snowflake Horizon Catalog"
    // feature. Typing the engine name should show the engine, not collapse to one
    // cell, so the row is hidden by neither filter.
    fireEvent.change(screen.getByRole('textbox', { name: 'Search features or engines by name' }), {
      target: { value: 'snowflake' },
    })
    await waitFor(() =>
      expect(screen.getByText(/Showing 1 engine matching “snowflake”/)).toBeInTheDocument()
    )
    const grid = screen.getByRole('grid')
    expect(within(grid).getByText('Position Deletes')).toBeInTheDocument()
    expect(within(grid).getByText('Snowflake Horizon Catalog')).toBeInTheDocument()
  })

  it('still filters feature rows when the text matches no engine', async () => {
    render(<App />)
    fireEvent.change(screen.getByRole('textbox', { name: 'Search features or engines by name' }), {
      target: { value: 'horizon' },
    })
    await waitFor(() =>
      expect(screen.queryByText('Hidden Partitioning')).not.toBeInTheDocument()
    )
    expect(within(screen.getByRole('grid')).getByText('Snowflake Horizon Catalog')).toBeInTheDocument()
    // No engine matched, so no "Showing N engines" hint and all columns remain.
    expect(screen.queryByText(/Showing \d+ engine/)).not.toBeInTheDocument()
    expect(within(screen.getByRole('grid')).getAllByText(/Athena/).length).toBeGreaterThan(0)
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

describe('Snowflake storage mode toggle', () => {
  it('defaults to Snowflake-managed storage in the Engines view', () => {
    render(<App />)
    // The pill sits in the Snowflake group header and offers the switch to
    // the external-volume dataset.
    expect(
      screen.getByRole('button', { name: 'Switch to External storage' }),
    ).toBeInTheDocument()
  })

  it('toggles between Snowflake and External storage data', () => {
    render(<App />)

    fireEvent.click(screen.getByRole('button', { name: 'Switch to External storage' }))
    // The label flips: the button now offers the way back.
    expect(
      screen.getByRole('button', { name: 'Switch to Snowflake storage' }),
    ).toBeInTheDocument()
    // The Snowflake engine column is still rendered (same platform id in both
    // modes, so filters and the column survive the toggle).
    expect(screen.getByRole('grid')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Switch to Snowflake storage' }))
    expect(
      screen.getByRole('button', { name: 'Switch to External storage' }),
    ).toBeInTheDocument()
  })

  it('renders no Snowflake storage toggle in the Catalogs view', async () => {
    render(<App />)
    await openView('Catalogs')
    expect(
      screen.queryByRole('button', { name: /storage$/ }),
    ).not.toBeInTheDocument()
  })
})

describe('Shareable URL', () => {
  it('opens the view and filters a link names', async () => {
    window.history.replaceState(null, '', '/?view=catalogs&q=polaris')
    render(<App />)
    await waitFor(() => expect(screen.queryByText('Loading…')).not.toBeInTheDocument())
    expect(screen.getByRole('tab', { name: 'Catalogs' })).toHaveAttribute('aria-selected', 'true')
    expect(screen.getByLabelText(/^Search/)).toHaveValue('polaris')
  })

  it('writes filter changes to the address bar', () => {
    render(<App />)
    fireEvent.click(screen.getByRole('button', { name: 'Filter by Partitioning' }))
    expect(window.location.search).toBe('?cat=partitioning')
  })

  it('ignores platforms the dataset does not have', () => {
    window.history.replaceState(null, '', '/?p=duckdb,no-such-engine')
    render(<App />)
    expect(screen.getAllByText(/DuckDB/).length).toBeGreaterThan(0)
    expect(window.location.search).toBe('?p=duckdb')
  })
})
