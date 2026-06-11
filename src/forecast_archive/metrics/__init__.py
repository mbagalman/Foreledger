"""Accuracy metrics: built-ins and the registerable metric protocol (ADR-004).

Built-ins (MAE/RMSE/MAPE/MASE) are implemented *as* protocol-conforming
metrics — one code path. Registered custom metrics must respect the
summarizable contract to be precomputed, and run isolated (timeout / error
guard) so a bad metric cannot corrupt or hang a recompute.
"""
