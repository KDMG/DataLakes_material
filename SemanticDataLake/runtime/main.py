from cmd import Cmd
import re
from models.DL_Graph import DL_Graph
import pandas as pd
import executors.mapper_auto as mapper_auto
from os.path import exists

DATASET_FOLDER = "../datasets/"
GRAPH_FOLDER = "../kg/"
GRAPH = "knowledge_graph_D5_L3_10.ttl"
METAGRAPH_FOLDER = "../mg/"
METAGRAPH = "metadata.ttl"
LOG_DIRECTORY = "../logs/"


class MyPrompt(Cmd):
    prompt = "dl> "
    intro = "Type ? to list commands, quit to exit the console."

    #########################################################################
    # Command methods
    #########################################################################
    def do_clean(self, inp):
        if inp == "all":
            check = input("Metadata layer is being cleared. Do you confirm? [Y/N] ")
            if check == "Y":
                if mg.clear_all():
                    print("The metadata layer has been reset.")
                else:
                    print("Some issues occurred.")

    def do_describe(self, inp):
        selected_source = mg.get_selected_source()
        if selected_source is not None:
            infos = mg.get_info_source(selected_source)
            print("Loading date: " + str(infos["date"]))
            print("Num. Items: " + str(infos["items"]))
            print("Num. Domains: " + str(infos["domains"]))
            domains = mg.get_mapped_domains(selected_source)
            print("Num. Mapped domains: " + str(len(domains)))

            for i, d in enumerate(domains):
                print(str(i) + ") " + str(d))
                print("\tmapped to " + str(domains[d]))
                completeness_level = dlGraph.eval_completeness(d)
                if completeness_level >= 0:
                    print("\tcompleteness level = "+str(completeness_level*100)+"%")
                else:
                    print("Error in the computation of the completeness level")

        else:
            print("No source in use.")

    def do_focus(self, inp):
        mg.select_domain(inp)

    def do_mount(self, file_path, remount=False):
        real_path = DATASET_FOLDER + file_path
        if real_path not in mg.get_all_paths():
            if exists(real_path):

                # Initialize LSHEnsemble
                lshensemble = mapper_auto.initialize_lsh(kg)

                # Import the file
                df = pd.read_csv(real_path)
                num_rows = len(df.index)
                num_columns = len(df.columns)
                all_sources = mg.get_sources()
                filename = file_path[: file_path.rfind('.')]
                uri_to_save = filename
                counter = 1
                while uri_to_save in all_sources:
                    uri_to_save = uri_to_save + "_" + counter
                # Add the source in the Metadata Graph
                mg.add_source(uri_to_save, num_rows, df.columns[1:], real_path)
                # Process each column
                for col in df.columns:
                    # Extract value list
                    values = df[col].astype('str').tolist()
                    # Discover the mapping
                    mappings = mapper_auto.map_source_domain(values, lshensemble)
                    list_mappings = list(mappings)
                    if len(list_mappings) > 0:
                        level = list_mappings[0]
                        mg.map(uri_to_save, col, level)
                        members = kg.get_members_from_level(level, fragmentLevel=False, fragmentOutput=False)
                        # Calculate and process the profiles for the column
                        profile = mapper_auto.calculate_profile(values, members)
                        for p, freq in profile.items():
                            # Store a profile item
                            mg.add_profile(uri_to_save, level, p, freq)
                # Store the updated Metadata Graph
                mg.serialize()
                if not remount:
                    print("The source has been mounted.")
                else:
                    print("The source has been synchronized.")
            else:
                print("The specified file does not exist.")
        else:
            print("The source is already mounted.")

    def do_sync(self, inp):
        try:
            print("Synchronizing the source...")
            info = mg.get_info_source(mg.selected_source)
            mg.clear()
            self.do_mount(info["location"], remount=True)
        except Exception as e:
            print(e)
            print("Some issues occurred.")

    def do_profile(self, inp):
        selected_source = mg.get_selected_source()
        if selected_source is not None:
            # Profile all domains
            if inp == "all":
                domains = mg.get_mapped_domains(selected_source)
                for d in domains:
                    profile = mg.get_profiles(d)
                    self._show_profile(d, profile)
            # Profile the selected domain
            else:
                selected_domain = mg.get_selected_domain()
                if selected_domain is not None:
                    # Profile the domain at its level
                    if inp == "":
                        level = mg.get_mapped_level(selected_domain)
                        members = kg.get_members_from_level(level, fragmentLevel=True, fragmentOutput=False)
                        profile = mg.get_profiles(selected_domain)
                        self._show_profile(selected_domain, profile)
                        mg.get_vect_profiles(selected_domain, members)

                    # Profile a domain with roll up
                    elif re.search("^up$", inp) is not None:
                        profile = dlGraph.get_profile_up(selected_domain)
                        self._show_profile(selected_domain, profile)
                else:
                    print("No focus on a domain.")
        else:
            print("No source in use.")

    def do_quit(self, inp):
        print("Bye")
        return True

    def do_sources(self, inp):
        sources = mg.get_sources()
        if len(sources) == 0:
            print("No source available.")
            return
        for i, s in enumerate(sources):
            print("\n" + str(i) + ") " + str(s))
            infos = mg.get_info_source(s)
            print("Location: " + str(infos["location"]))
        print("")

    def do_unmount(self, inp):
        result = mg.clear()
        if result:
            print("The source has been unmounted.")
            mg.serialize()
        else:
            print("Some error occurred while unmounting the source.")

    def do_use(self, inp):
        mg.select_source(inp)

    #########################################################################
    # Help methods
    #########################################################################
    def help_clean(self):
        print("Remove all sources from the Metadata layer.\nUsage: clean all")
    def help_describe(self):
        print("Provides the description of the source schema and mappings for the selected source.\nUsage: describe")

    def help_focus(self):
        print(
            "Select a domain of the selected source on which the following commands will be executed.\nUsage: focus {ID_COLUMN | URI}")

    def help_mount(self):
        print("Mount a new source in the Data Lake.\nUsage: mount FILEPATH")

    def help_profile(self):
        print(
            "Shows the profile of the selected domain from the selected source. With 'up' shows the aggregated "
            "profile.\nUsage: profile [up]")

    def help_quit(self):
        print("Exits the console.\nUsage: quit")

    def help_sources(self):
        print("List the sources loaded in the Data Lake.\nUsage: sources ")

    def help_sync(self):
        print("Synchronize the metadata for the selected source by performing unumount and mount.\nUsage: sync")
    def help_unmount(self):
        print("Unmount the selected source.\nUsage: unmount")

    def help_use(self):
        print("Select a source on which the following commands will be executed.\nUsage: use {ID_SOURCE | URI}")

    do_EOF = do_quit
    help_EOF = help_quit

    def _show_profile(self, d, profile):
        print("Domain: " + str(d))
        for k, v in profile.items():
            print("\t" + str(k) + ": " + str(v))


def main():
    print("############################################")
    print("Semantic Data Lake management console v. 0.1")
    print("############################################")
    global kg, mg, dlGraph
    print("\nBooting...")
    print("Importing the Knowledge Graph...")
    kg_file = GRAPH_FOLDER + GRAPH
    print("Importing the Metadata graph...\n")
    mg_file = METAGRAPH_FOLDER + METAGRAPH
    dlGraph = DL_Graph(kg=kg_file, mg=mg_file)
    mg = dlGraph.mg
    kg = dlGraph.kg
    # Run the prompt
    MyPrompt().cmdloop()


if __name__ == "__main__":
    main()
