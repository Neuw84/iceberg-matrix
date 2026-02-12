import { render, screen } from '@testing-library/react'
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
