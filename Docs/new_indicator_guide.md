# Creating a New Indicator

This guide walks through adding a brand-new environmental indicator to CEPALSTAT, from creating it in the CEPALSTAT Admin area to publishing it in the thematic tree. It complements the annual update process described in the [README](../README.md), which covers indicators that already exist.

## Overview

| Step | Where |
|---------------------------------|-----------------------------|
| 1\. Create the indicator | CEPALSTAT Admin |
| 2\. Define the dimensional structure | CEPALSTAT Admin |
| 3\. Add the indicator to the metadata table | `Data/indicator_metadata.xlsx` |
| 4\. Write the processing code | R |
| 5\. Check the data | QC dashboard |
| 6\. Upload the data | Wasabi |
| 7\. Request publication in the thematic tree | Email to Andrés Yañez |
| 8\. Make the indicator visible | CEPALSTAT Admin |
| 9\. Complete the technical sheet | CEPALSTAT Admin |
| 10\. Polish the metadata with AI (optional) | `Scripts/run_meta.R` |

------------------------------------------------------------------------

## 1. Create the indicator in the CEPALSTAT Admin area

1.  Select the **Área temática**.
2.  Enter the **Descripción en español** and the **Descripción en inglés**.
3.  Save. This generates the new indicator. **Write down the new indicator number**; it is used in every step that follows.

These are the only required fields at this stage. The rest of the technical sheet is completed in [Step 9](#9-complete-the-technical-sheet).

## 2. Define the dimensional structure in the CEPALSTAT Admin area

1.  Open the indicator's **Estructura dimensional**.
2.  Assign the dimensions. The typical setup is:
    - **Eje horizontal:** `Años_ESTANDAR`
    - **Eje vertical:** `País_ESTANDAR`
    - **Encabezado:** any additional dimension (e.g., sector, type)
3.  Under **Seleccione desagregaciones**, select all values of all dimensions, then click **OK**.

## 3. Add the indicator to the metadata table

Add a row for the new indicator to `Data/indicator_metadata.xlsx`.

- **Required for the R code to run:** `id`, `indicator`, and `source`. If the indicator is missing from this table, `process_indicator()` stops with an error asking you to add it.
- **Recommended:** complete the remaining columns now, while the details are at hand.

Save and close the file, then restart R (or re-source `Scripts/utils.R`) so the updated table is loaded.

## 4. Write the processing code

1.  Download the source data and write the indicator's processing functions: `filter_*`, `transform_*`, and the `calculate_regional` strategy.
2.  Because the indicator is new, the spec needs two additional settings:
    - **`define_source`:** a function that assigns the indicator's source ID. The default (`existing_source`) looks up the source already published in CEPALSTAT, which a new indicator does not have yet.
    - **`new_indicator = TRUE`:** skips the comparisons against published data, since none exists yet.
3.  Run the indicator from `Scripts/run_all.R` as usual, e.g., `run_one_indicator(5805)`.

### Example spec

``` r
# Assign the source: Global Information System on Water and Agriculture (AQUASTAT)
define_source_aquastat <- function(df, indicator_id) {
  df %>% mutate(source_id = 5173)
}

spec_5805 <- indicator_spec(
  indicator_id = 5805,
  data = aquastat,
  max_year = max_year_fao,
  dim_config = dim_config_5805,
  filter_data = filter_5805,
  transform_data = transform_5805,
  calculate_regional = maintain_regional,
  define_source = define_source_aquastat, # new indicator: no published source to look up
  new_indicator = TRUE                    # new indicator: no published data to compare against
)
```

## 5. Check the data in the QC dashboard

Run `Scripts/dashboard/dashboard.qmd` and load the new indicator.

Because there is no published data yet, the comparison views (e.g., Value Comparisons, Value Differences) will be empty. Use the remaining views to sanity-check the data instead:

- Does the Latin America and the Caribbean total look reasonable?
- Are the country series consistent over time?
- Do a few spot-checked values match the raw source data?

## 6. Upload the data to Wasabi

As with any indicator, select the new file from `Data/Cleaned` in Wasabi and upload it.

If you receive an access error, email Andrés with the new indicator number.

## 7. Request publication in the thematic tree

Only Andrés can publish an indicator in the CEPALSTAT thematic tree. Email him the following:

- The **indicator number**
- **Where it belongs in the tree:** at minimum, the name of the area (e.g., "Emissions of greenhouse gases") and its position within it (e.g., "at the top of the list")

**Tip:** ask Andrés for a copy of the thematic tree and keep it on file. With it, you can phrase the request in the terms he uses, for example: `area_id = 732 (Emissions of greenhouse gases), indicator_order = 75`.

## 8. Make the indicator visible

While waiting for Andrés, open the indicator in the CEPALSTAT Admin area (**Editar**) and check the red box at the top: **Indicador visible al usuario externo**.

An indicator appears in the CEPALSTAT tree only once it's **published** (done by Andrés) and it's **visible** (done by you).

## 9. Complete the technical sheet

Once the indicator is published and visible, the changes take a couple of hours to appear in CEPALSTAT. You can use this time to finish the technical sheet (the indicator's **Editar** page in the CEPALSTAT Admin area).

| Field | Guidance |
|------------------------------------|------------------------------------|
| **Orden de despliegue** | Should match the position you gave Andrés - this is for its internal ordering in the Admin area. |
| **Unidad de medida** | The unit of the published values. Double-check it against the calculation in the R code. |
| **Características** | Almost always: *Periodicity: Annual / Coverage: National* (EN) and *Periodicidad: Anual / Cobertura: Nacional* (ES). |
| **Definición** | Write a complete first draft with all the necessary technical information. Content matters more than style at this stage, since these three fields can be polished in Step 10. |
| **Metodología** | Same as above. |
| **Comentarios** | Same as above. |

## 10. Polish the metadata with AI (optional)

1.  Run `Scripts/run_meta.R` to generate suggested improvements to the Definición, Metodología, and Comentarios fields (see the README's [Revise metadata](../README.md#5-revise-metadata-optional) section).
2.  Review the suggestions, then enter the final text in the CEPALSTAT Admin area.
