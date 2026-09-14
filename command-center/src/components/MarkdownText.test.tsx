import { render, screen, within } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { MarkdownText } from './MarkdownText'

describe('readable recorded Markdown', () => {
  it('renders headings, emphasis, code, paragraphs and lists without raw Markdown markers', () => {
    const { container } = render(<MarkdownText text={'## Observed failure\n\n**Dataset refresh** failed for `Sales_Model`.\n\nMissing evidence:\n\n- Workspace ID\n- Refresh history'} />)
    expect(screen.getByRole('heading', { name: 'Observed failure', level: 4 })).toBeTruthy()
    expect(container.querySelector('strong')?.textContent).toBe('Dataset refresh')
    expect(container.querySelector('code')?.textContent).toBe('Sales_Model')
    expect(within(screen.getByRole('list')).getAllByRole('listitem')).toHaveLength(2)
    expect(container.textContent).not.toContain('**')
    expect(container.textContent).not.toContain('`')
    expect(container.querySelector('h1,h2,h3')).toBeNull()
  })

  it('renders a readable table and safe external documentation links', () => {
    render(<MarkdownText text={'| Evidence | Status |\n| --- | --- |\n| Refresh | Missing |\n\n[Documentation](https://learn.microsoft.com/power-bi/)'} />)
    expect(screen.getByRole('table')).toBeTruthy()
    const link = screen.getByRole('link', { name: 'Documentation' })
    expect(link.getAttribute('href')).toBe('https://learn.microsoft.com/power-bi/')
    expect(link.getAttribute('rel')).toBe('noopener noreferrer')
    expect(link.getAttribute('target')).toBe('_blank')
  })

  it('does not render raw HTML, embedded images, scripts or unsafe links', () => {
    const { container } = render(<MarkdownText text={[
      '<script>window.compromised = true</script>',
      '<img src="https://example.test/track" onerror="alert(1)">',
      '<iframe src="https://example.test"></iframe>',
      '![tracking image](https://example.test/pixel)',
      '[unsafe](javascript:alert%281%29)',
      '[data](data:text/html,bad)',
      '[encoded](javascript&#58;alert%281%29)',
      '[relative](/api/admin/users)',
    ].join('\n\n')} />)
    expect(container.querySelector('script,img,iframe,style,object,input,a')).toBeNull()
    expect(screen.getByText('unsafe')).toBeTruthy()
    expect(screen.getByText('encoded')).toBeTruthy()
  })

  it('keeps run-list previews valid inside a button without nested links or block content', () => {
    const { container } = render(<button type="button"><MarkdownText preview text={'# Summary\n\n**Failure** for `dataset`.\n\n- First check\n- [Second check](https://learn.microsoft.com/)'} /></button>)
    const button = screen.getByRole('button')
    expect(within(button).getByText('Failure')).toBeTruthy()
    expect(button.textContent).toContain('First check')
    expect(container.querySelector('a,p,h1,h2,h3,h4,h5,h6,ul,ol,li,div')).toBeNull()
  })
})
