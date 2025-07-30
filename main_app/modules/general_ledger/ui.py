from shiny import ui

print("DEBUG — general_ledger_ui() loaded")

def general_ledger_ui():
    print("DEBUG — general_ledger_ui() function called")

    ui.output_ui("gl_view")
