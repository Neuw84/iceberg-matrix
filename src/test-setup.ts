import '@testing-library/jest-dom'
import { configure } from '@testing-library/react'
import { afterEach } from 'vitest'

// findBy*/waitFor give up after 1s by default. Several tests wait for the
// lazily loaded Compare panel or detail popover (a separate chunk), which can
// take longer than that on a slow CI runner and then fails with "Unable to find
// an element" even though nothing is wrong. Allow more time; a test that
// succeeds does not wait any longer than it needs to.
configure({ asyncUtilTimeout: 10000 })

// App mirrors its filters into the query string, and jsdom keeps the URL
// between tests: reset it so one test's filters never open the next one's App.
afterEach(() => {
  window.history.replaceState(null, '', '/')
})
