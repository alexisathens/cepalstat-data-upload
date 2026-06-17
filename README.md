# cepalstat-data-upload

Automates the collection, cleaning, and quality control of environmental indicators for the CEPALSTAT database.

## Process Flow

```
01 Download  →  02 Clean  →  03 QC Report  →  04 Technical Sheet  →  Upload to Wasabi
```

1. **Download raw data** (`Scripts/01_*`) — fetch data from external sources (FAO, Climate Watch, EM-DAT, OLADE, InforMEA)
2. **Clean data** (`Scripts/02_*`) — standardize, filter, and transform to CEPALSTAT format
3. **Run QC report** (`Scripts/03_qc_report.qmd`) — compare new data against currently published data and flag discrepancies
4. **Review QC output** — check `QC Reports/` for missing entries, new entries, and large value changes (>20%)
5. **Backup existing data** — download the *plantilla de ingresos* from the data management system before overwriting
6. **Generate technical sheet** (`Scripts/04_technical_sheet.R`) — prepare metadata documentation
7. **Upload to Wasabi** — upload validated files from `Data/Cleaned/`

## Scripts

| Script | Purpose |
|---|---|
| `01_climatewatch.R` | Download Climate Watch emissions data via API |
| `01_olade.R` | Download OLADE energy data |
| `01_informea.qmd` | Download InforMEA environmental treaty data |
| `02_climatewatch.R` | Clean and standardize Climate Watch indicators |
| `02_emdat.R` | Clean EM-DAT natural disaster data |
| `02_olade.R` | Clean and standardize OLADE energy indicators |
| `02_other.R` | Clean miscellaneous indicators |
| `0102_fao.R` | Download and clean FAO data (FAOSTAT, FishStat, Aquastat) |
| `03_qc_report.qmd` | Generate QC HTML report comparing new vs. published data |
| `04_technical_sheet.R` | Generate technical metadata sheet for upload |
| `utils.R` | Shared utility functions (API calls, formatting, validation) |
| `process_indicator_fn.R` | Core `process_indicator()` function used by all cleaning scripts |
| `build_iso_table.R` | Build/update country name → ISO code mapping (`Data/iso_codes.xlsx`) |
| `build_metadata_table.R` | Build/update indicator metadata table (`Data/indicator_metadata.xlsx`) |

Each source also has a companion instruction file (`01_*_instructions.qmd`) with source-specific download steps and notes.

## The `process_indicator()` Function

The `process_indicator_fn.R` script defines the central function used by all `02_*` cleaning scripts. It handles the full pipeline in a standardized way:

![process_indicator workflow](Docs/process_indicator_flowchart.png)

Steps: filter raw data → standardize country names → create LAC regional totals → harmonize labels with published data → join dimension members → attach footnotes/sources → format for Wasabi → export cleaned data and comparison files → update metadata.

## Data Sources

| Source | Data | Download method |
|---|---|---|
| FAO (FAOSTAT) | Land use, climate change, land cover, crops/livestock, fertilizers, pesticides | Automated via `FAOSTAT` R package |
| FAO (FishStat / Aquastat) | Fish capture, aquaculture, water withdrawal | Manual download required |
| Climate Watch | GHG emissions (CO₂, CH₄, N₂O, etc.) | Automated via API |
| EM-DAT | Natural disaster deaths, affected, economic damage | Manual download (account required at emdat.be) |
| OLADE | Energy production and consumption | Manual download |
| InforMEA | Environmental treaty indicators | See `01_informea.qmd` |

## Key Files

```
Scripts/          # All R and Quarto scripts
Data/
├── Raw/          # Raw source downloads
├── Cleaned/      # Final files for Wasabi upload (id{indicator_id}_{timestamp}.xlsx)
├── Checks/       # Comparison files vs. published data (comp_id{indicator_id}.xlsx)
├── iso_codes.xlsx           # Country name ↔ CEPALSTAT ID mapping
└── indicator_metadata.xlsx  # Indicator tracking and processing history
Docs/
├── process_indicator_flowchart.png  # Visual diagram of process_indicator()
└── qc_report_good.html              # Sample QC reports (good / bad examples)
QC Reports/       # Generated QC HTML reports
```

## Dependencies

Core R packages: `tidyverse`, `magrittr`, `httr2`, `jsonlite`, `readxl`, `writexl`, `CepalStatR`, `FAOSTAT`, `quarto`, `assertthat`
