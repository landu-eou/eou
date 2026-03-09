from __future__ import annotations

"""
Configuración de logging del pipeline.

Este módulo mantiene la misma interfaz que ya usa el resto del código,
pero desactiva la emisión de logs hacia stdout/stderr.

Motivo:
- El workflow ya está verificado y no se desea mostrar trazas informativas.
- Se conserva el logger para no cambiar la lógica ni la estructura del resto
  de módulos.
"""

import logging


LOGGER_NAME = "eou_corporationKills"


def configure_logging() -> logging.Logger:
    """
    Devuelve un logger silencioso.

    Se usa NullHandler para que las llamadas logger.info/logger.warning no
    produzcan salida visible durante la ejecución del workflow.
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.CRITICAL + 1)
    logger.handlers.clear()
    logger.propagate = False
    logger.addHandler(logging.NullHandler())
    return logger
