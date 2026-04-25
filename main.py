import helper_functions as helper

def main():
    choice = helper.print_main_menu()
    exit_choice = "8"

    # Dictionary to convert user input into a function
    top_level_functions = {
        "1": helper.rent_item,
        "2": helper.return_item,
        "3": helper.grant_extension,
        "4": helper.search_tables,
        "5": helper.add_item,
        "6": helper.add_customer,
        "7": helper.edit_customer,
    }

    # Main loop
    while choice != exit_choice:
        if choice in top_level_functions.keys():
            top_level_functions[choice]()

        else:
            print("Choice unrecognised")

        helper.save_changes()
        print()
        choice = helper.print_main_menu()

    helper.close_connection()
    print("Successfully saved changes, Goodbye!")
    print()


if __name__ == '__main__':
    main()
