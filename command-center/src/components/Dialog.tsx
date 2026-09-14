import { useEffect, useId, useRef } from 'react'
import type { ReactNode } from 'react'
import { X } from 'lucide-react'

export function Dialog({ open, title, eyebrow, onClose, children, busy = false, drawer = false }: {
  open: boolean
  title: string
  eyebrow: string
  onClose: () => void
  children: ReactNode
  busy?: boolean
  drawer?: boolean
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const titleId = useId()
  useEffect(() => {
    const dialog = ref.current
    if (!dialog) return
    if (open && !dialog.open) dialog.showModal()
    if (!open && dialog.open) dialog.close()
    return () => { if (dialog.open) dialog.close() }
  }, [open])

  return <dialog
    ref={ref}
    className={drawer ? 'dialog drawer' : 'dialog'}
    aria-labelledby={titleId}
    onCancel={(event) => { event.preventDefault(); if (!busy) onClose() }}
  >
    <header className="dialog-heading">
      <div><span className="eyebrow">{eyebrow}</span><h2 id={titleId}>{title}</h2></div>
      <button type="button" className="icon-button" onClick={onClose} disabled={busy} aria-label={`Close ${title}`}><X size={20} /></button>
    </header>
    <div className="dialog-content">{children}</div>
  </dialog>
}
