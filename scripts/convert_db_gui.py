"""Windows drag-and-drop GUI entry point for convert_db.py."""

from __future__ import annotations

import queue
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox

from convert_db import convert_source


def main() -> None:
    root = tk.Tk()
    root.title("EVE SQLite Converter")
    root.resizable(False, False)
    root.withdraw()

    sources = [Path(arg) for arg in sys.argv[1:] if arg.lower().endswith((".db", ".db.zst"))]
    if not sources:
        selected = filedialog.askopenfilenames(
            parent=root,
            title="Selecciona una base SQLite",
            filetypes=[("SQLite", "*.db *.db.zst"), ("Todos los archivos", "*.*")],
        )
        sources = [Path(path) for path in selected]
    if not sources:
        root.destroy()
        return

    root.geometry("480x110")
    status = tk.StringVar(value=f"Preparando {len(sources)} archivo(s)…")
    tk.Label(root, textvariable=status, wraplength=440, justify="left", padx=18, pady=24).pack(fill="both", expand=True)
    root.deiconify()

    events: queue.Queue[tuple[str, object]] = queue.Queue()

    def worker() -> None:
        results = []
        for index, source in enumerate(sources, start=1):
            events.put(("progress", f"Convirtiendo {index}/{len(sources)}: {source.name}"))
            try:
                outputs = convert_source(source)
                results.append((source, outputs, None))
            except Exception as exc:
                results.append((source, (), str(exc)))
        events.put(("finished", results))

    def poll() -> None:
        try:
            while True:
                kind, value = events.get_nowait()
                if kind == "progress":
                    status.set(str(value))
                else:
                    results = value
                    successes = [entry for entry in results if entry[2] is None]
                    failures = [entry for entry in results if entry[2] is not None]
                    summary = [f"Convertidos correctamente: {len(successes)}"]
                    summary.extend(f"  {path.parent}: {', '.join(item.name for item in outputs)}" for path, outputs, _ in successes)
                    summary.extend(f"Error en {path}: {error}" for path, _outputs, error in failures)
                    if failures:
                        messagebox.showerror("Conversión terminada con errores", "\n".join(summary), parent=root)
                    else:
                        messagebox.showinfo("Conversión terminada", "\n".join(summary), parent=root)
                    root.destroy()
                    return
        except queue.Empty:
            root.after(250, poll)

    threading.Thread(target=worker, daemon=True).start()
    root.after(250, poll)
    root.mainloop()


if __name__ == "__main__":
    main()
