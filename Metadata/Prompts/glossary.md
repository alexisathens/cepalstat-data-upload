---
description: Required English to Spanish terminology for finish_draft() in update_metadata_fn.R. Only the table rows are sent to the model.
---
# Translation glossary

Each row tells the translator which Spanish term to use for an English term. The glossary takes precedence over an indicator's existing Spanish metadata and the reference examples.

How to use:
- Add one row per term. Keep the header row and the `|---|` line as they are.
- **English** and **Spanish** must be the exact terms. After each translation, the script checks that every English term found in the draft has its Spanish term in the translation, and lists any that are missing.
- **Notes** (optional) give the translator extra guidance, such as a variant to avoid.
- Do not use the `|` character inside a cell.

| English                         | Spanish                    | Notes                                                                                                                                                |
| ------------------------------- | -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| LULUCF                          | LULUCF                     | Keep the English acronym. Do not use UTCUTS. If the term is written out, use "uso de la tierra, cambio de uso de la tierra y silvicultura (LULUCF)". |
| ECLAC                           | CEPAL                      |                                                                                                                                                      |
| Latin America and the Caribbean | América Latina y el Caribe |                                                                                                                                                      |
| GHG                             | GEI                        |                                                                                                                                                      |
