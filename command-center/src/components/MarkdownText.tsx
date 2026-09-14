import Markdown from 'react-markdown'
import type { Components } from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { safeSourceUrl } from '../domain'
import './MarkdownText.css'

const elements = [
  'p', 'br', 'strong', 'em', 'del', 'code', 'pre', 'blockquote', 'ul', 'ol', 'li',
  'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'hr', 'a', 'table', 'thead', 'tbody', 'tr', 'th', 'td',
]
const components: Components = {
  h1: 'h4', h2: 'h4', h3: 'h4', h4: 'h4', h5: 'h5', h6: 'h6',
  a: ({ children, href }) => href
    ? <a href={href} target="_blank" rel="noopener noreferrer">{children}</a>
    : <span>{children}</span>,
  table: ({ children }) => <div className="markdown-table"><table>{children}</table></div>,
}
const previewComponents: Components = {
  p: ({ children }) => <span>{children}{' '}</span>,
}

export function MarkdownText({ text, preview = false }: { text: string; preview?: boolean }) {
  const content = <Markdown
    remarkPlugins={[remarkGfm]}
    skipHtml
    allowedElements={preview ? ['p', 'br', 'strong', 'em', 'del', 'code'] : elements}
    unwrapDisallowed
    components={preview ? previewComponents : components}
    urlTransform={(url) => safeSourceUrl(url) ?? ''}
  >{text}</Markdown>
  // Run-list rows are buttons; their preview must contain no nested links or block elements.
  return preview
    ? <span className="markdown-preview">{content}</span>
    : <div className="markdown-text">{content}</div>
}
