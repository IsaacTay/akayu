# stolen from gsdp


def _coerce_ikdc_list(ikdc):
    if ikdc is None:
        return None
    try:
        values = [float(x) for x in ikdc]
    except (TypeError, ValueError):
        return None
    if len(values) < 20:
        return None
    return values[:20]


def summarize_ikdc(ikdc):
    values = _coerce_ikdc_list(ikdc)
    if values is None:
        return None

    ikdc_total = float(values[0])
    ikdc_symptoms = float(sum(values[1:8]))
    ikdc_sport = float(sum(values[8:18]))
    ikdc_function = float(values[19])

    ikdc_total_perc = round((ikdc_total / 87) * 100, 1)
    ikdc_symptoms_perc = round((ikdc_symptoms / 37) * 100, 1)
    ikdc_sport_perc = round((ikdc_sport / 40) * 100, 1)
    ikdc_function_perc = round((ikdc_function / 10) * 100, 1)

    if ikdc_total_perc < 60:
        ikdc_class = "Severe"
    elif ikdc_total_perc <= 85:
        ikdc_class = "Moderate"
    elif ikdc_total_perc <= 100:
        ikdc_class = "Functional"
    else:
        ikdc_class = ""

    return {
        "ikdc": [int(x) if float(x).is_integer() else x for x in values],
        "total": ikdc_total,
        "symptoms": ikdc_symptoms,
        "sport": ikdc_sport,
        "function": ikdc_function,
        "total_pct": ikdc_total_perc,
        "symptoms_pct": ikdc_symptoms_perc,
        "sport_pct": ikdc_sport_perc,
        "function_pct": ikdc_function_perc,
        "class": ikdc_class,
    }


def set_ikdc(ikdc, report_html_filename):
    summary = summarize_ikdc(ikdc)
    if summary is None:
        return None

    ikdc_summary = (
        '<ul><li>Patient\'s IKDC score: <u class="colorText">'
        + str(summary["total_pct"])
        + "%</u><br>&nbsp;<br>&nbsp;<br></li></ul>"
    )
    ikdc_overall = (
        "("
        + str(summary["total"])
        + " / 87) x 100% = "
        + str(summary["total_pct"])
        + "%"
    )
    ikdc_symptoms = (
        "("
        + str(summary["symptoms"])
        + " / 37) x 100% = "
        + str(summary["symptoms_pct"])
        + "%"
    )
    ikdc_sport = (
        "("
        + str(summary["sport"])
        + " / 40) x 100% = "
        + str(summary["sport_pct"])
        + "%"
    )
    ikdc_function = (
        "("
        + str(summary["function"])
        + " / 10) x 100% = "
        + str(summary["function_pct"])
        + "%"
    )

    ikdc_overall = ikdc_overall + "<br>(" + summary["class"] + ")<sup><i>#</i></sup>"

    print("\n" + ikdc_summary + "\n")
    print("IKDC        " + ikdc_overall + "\n")
    print("Symptom     " + ikdc_symptoms + "\n")
    print("Sport       " + ikdc_sport + "\n")
    print("Function    " + ikdc_function + "\n")
    return summary