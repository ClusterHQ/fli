/*
 * Copyright 2016 ClusterHQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * CODE GENERATED AUTOMATICALLY WITH github.com/ClusterHQ/go/client/cmd/cmdgen
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */

package cli

import "strings"
import "encoding/json"
import "text/tabwriter"
import "bytes"
import "fmt"
import "os"
import "github.com/spf13/cobra"

func getMultiUseLine(sb string, mu []string) string {
	var o = sb + " ;"

	for i, m := range mu {
		o += m
		if i+1 < len(mu) {
			o += ";"
		}
	}

	return o
}

func multiUseLine(c string, u string) string {
	var o = ""
	var m = strings.Split(u, ";")

	if len(m) == 1 {
		return "  " + c + " " + u
	}

	for i, s := range m[1:] {
		o += "  " + c + " " + s

		if i+1 < len(m[1:]) {
			o += "\n"
		}
	}

	return o
}

// Generate the table as CLI output
func genTable(tab [][]string) string {
	if len(tab) == 0 {
		return ""
	}

	var wr = new(tabwriter.Writer)
	var buf = bytes.NewBuffer([]byte{})

	wr.Init(buf, 0, 0, 1, ' ', 0)

	for _, row := range tab[:] {
		for i, cell := range row {
			if cell == "" {
				cell = "-"
			}

			fmt.Fprintf(wr, "%s", cell)
			if i < (len(row) - 1) {
				fmt.Fprint(wr, "\t")
			} else {
				fmt.Fprintf(wr, "\n")
			}
		}
	}

	// flush to stdout
	wr.Flush()

	return string(buf.Bytes()[:])
}

func init() {
	cobra.AddTemplateFunc("multiUseLine", multiUseLine)
}
func newDpcliCmd(h handlerIface) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "dpcli",
		Short: "dpcli - data management utility",
		Long:  `dpcli is a utility to locally create and list volumesets, volumes, snapshots and branches. It also synchronizes state between the local client and the Volume Hub. Please refer to documentation on Volume Hub and specific commands for more information on these entities and the semantic meaning of various operations.`,
	}

	cmd.AddCommand(newDpcliCreateCmd(h))
	cmd.AddCommand(newDpcliShowCmd(h))
	cmd.AddCommand(newDpcliUpdateCmd(h))
	cmd.AddCommand(newDpcliRemoveCmd(h))
	cmd.AddCommand(newDpcliPushCmd(h))
	cmd.AddCommand(newDpcliPullCmd(h))
	cmd.AddCommand(newDpcliSyncCmd(h))
	cmd.AddCommand(newDpcliInitCmd(h))
	cmd.AddCommand(newDpcliSetCmd(h))
	cmd.AddCommand(newDpcliGetCmd(h))

	var outputF = ""
	var complCmd = &cobra.Command{
		Use:   getMultiUseLine("completion", []string{"(--output <filepath> | -o <filepath>)"}),
		Short: "Generates bash completion file for dpcli command",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Parent().GenBashCompletionFile(outputF)
		},
	}

	complCmd.Flags().StringVarP(&outputF, "output", "o", "", "Generates bash completions file for dpcli")
	cmd.AddCommand(complCmd)

	return cmd
}

func newDpcliCreateCmd(h handlerIface) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "create",
		Short: "Creates volumeset, volume or snapshot",
		Long:  `This subcommand creates a volumeset, a volume or a snapshot.`,
	}

	cmd.AddCommand(newCreateVolumesetCmd(h))
	cmd.AddCommand(newCreateSnapshotCmd(h))
	cmd.AddCommand(newCreateVolumeCmd(h))

	return cmd
}

func newCreateVolumesetCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volumeset", []string{
			"[(--attributes <key>=<value>,... | -a <key>=<value>,...)] [(--description <description> | -d <description>)] [<name>]",
		}),
		Short: "Create a new empty volumeset",
		Long:  `A new volumeset is created. The volumeset is empty, which means it does not contain any volume, snapshot or branch. The volumeset is not available on the Volume Hub until it is synced. Till then all the changes will remain local. The local meta-data store should be initialized before using this command. <name> is passed as an argument and is a heirarchical or flat name for the volumeset`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var attributesF string
			attributesF, err = cmd.Flags().GetString("attributes")
			if err != nil {
				return err
			}

			var descriptionF string
			descriptionF, err = cmd.Flags().GetString("description")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerCreateVolumeset(
				attributesF,
				descriptionF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("attributes", "a", "", "a comma separated list of key-value pairs")
	cmd.Flags().StringP("description", "d", "", "description for the volumeset")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newCreateSnapshotCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("snapshot", []string{
			"(--volume <volume id> | -V <volume id>) [(--branch <branch name> | -b <branch name>)] [(--attributes <key>=<value>,... | -a <key>=<value>,...)] [(--description <description> | -d <description>)] [<snapshot name>]",
		}),
		Short: "Create a new snapshot from a volume",
		Long:  `Creates a new snapshot of a volume identifier passed with --volume option. A --branch option is required to create a snapshot. If the branch name does not exists then it will be created in local meta-data store. It needs to be synchronized with the Volume Hub to be available for other clients to use. The newly created snapshot becomes the tip of the branch. The previous tip of the branch becomes the parent of the new snapshot. If a new branch was created then it does not have a parent. Any subsequent snapshot should be associated to the same branch.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var branchF string
			branchF, err = cmd.Flags().GetString("branch")
			if err != nil {
				return err
			}

			var volumeF string
			volumeF, err = cmd.Flags().GetString("volume")
			if err != nil {
				return err
			}

			var attributesF string
			attributesF, err = cmd.Flags().GetString("attributes")
			if err != nil {
				return err
			}

			var descriptionF string
			descriptionF, err = cmd.Flags().GetString("description")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerCreateSnapshot(
				branchF,
				volumeF,
				attributesF,
				descriptionF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("branch", "b", "", "branch name for the snapshot")
	cmd.Flags().StringP("volume", "V", "", "volume identifier to snapshot")
	cmd.Flags().StringP("attributes", "a", "", "a comma separated list of key-value pairs")
	cmd.Flags().StringP("description", "d", "", "description for the snapshot")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newCreateVolumeCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volume", []string{
			"(--snapshot <snapshot id> | -s <snapshot id>) [(--attributes <key>=<value>,... | -a <key>=<value>,...)] [<volume name>]",
			"(--volumeset <volumeset id> | -v <volumeset id>) [(--branch <branch name> | -b <branch name>)] [(--attributes <key>=<value>,... | -a <key>=<value>,...)] [<volume name>]",
		}),
		Short: "Create a new volume",
		Long:  `Creates an empty volume or a volume that is pre-populated with snapshot content. If only --volumeset option is given then creates an empty volume that is associated to the volumeset. If --branch and --volumeset option is used, then then volume is initiated from the tip of the branch . If only --snapshot is used then a volume is created from that snapshot.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var volumesetF string
			volumesetF, err = cmd.Flags().GetString("volumeset")
			if err != nil {
				return err
			}

			var branchF string
			branchF, err = cmd.Flags().GetString("branch")
			if err != nil {
				return err
			}

			var snapshotF string
			snapshotF, err = cmd.Flags().GetString("snapshot")
			if err != nil {
				return err
			}

			var attributesF string
			attributesF, err = cmd.Flags().GetString("attributes")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerCreateVolume(
				volumesetF,
				branchF,
				snapshotF,
				attributesF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("volumeset", "v", "", "volumeset identifer")
	cmd.Flags().StringP("branch", "b", "", "branch whose tip is used to create the volume. Used with --volumeset option")
	cmd.Flags().StringP("snapshot", "s", "", "snapshot identifer to create the volume")
	cmd.Flags().StringP("attributes", "a", "", "a comma separated list of key-value pairs")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newDpcliShowCmd(h handlerIface) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "show",
		Short: "Show the list of volumesets, volumes, branches or snapshots",
		Aliases: []string{
			"list",
		},
	}

	cmd.AddCommand(newShowVolumesetCmd(h))
	cmd.AddCommand(newShowSnapshotCmd(h))
	cmd.AddCommand(newShowBranchCmd(h))
	cmd.AddCommand(newShowVolumeCmd(h))

	return cmd
}

func newShowVolumesetCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volumeset", []string{
			"[<volumeset id> | <volumeset path>]",
		}),
		Short: "Show all the volumesets",
		Long:  `Lets you list all the volumesets`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var s []byte
			s, err = h.handlerShowVolumeset(
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newShowSnapshotCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("snapshot", []string{
			"(--volumeset <volumeset id> | -v <volumeset id>)",
		}),
		Short: "Show all snapshots for the given volumeset",
		Long:  `Lets you list all the snapshots for the given volumeset.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var volumesetF string
			volumesetF, err = cmd.Flags().GetString("volumeset")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerShowSnapshot(
				volumesetF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("volumeset", "v", "", "volumeset identifier to list snapshots")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newShowBranchCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("branch", []string{
			"(--volumeset <volumeset id> | -v <volumeset id>)",
		}),
		Short: "Show the branches for the given volumeset",
		Long:  `Lists all the branches for the given volumeset. If <regexpr> is used then lists the branches that match the <regexpr>.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var volumesetF string
			volumesetF, err = cmd.Flags().GetString("volumeset")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerShowBranch(
				volumesetF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("volumeset", "v", "", "volumeset identifier to list variants")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newShowVolumeCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volume", []string{
			"[(--volumeset <volumeset id> | -v <volumeset id>)]",
		}),
		Short: "Show the volumes",
		Long:  `Lets you list all the volumes. If --volumeset option is provided then lists all volumes for the give volumeset identifier.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var volumesetF string
			volumesetF, err = cmd.Flags().GetString("volumeset")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerShowVolume(
				volumesetF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("volumeset", "v", "", "volumeset id to list all volumes associated")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newDpcliUpdateCmd(h handlerIface) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "update",
		Short: "This command can be used to update metadata different objects based on the subcommands.",
	}

	cmd.AddCommand(newUpdateVolumesetCmd(h))
	cmd.AddCommand(newUpdateSnapshotCmd(h))
	cmd.AddCommand(newUpdateBranchCmd(h))
	cmd.AddCommand(newUpdateVolumeCmd(h))

	return cmd
}

func newUpdateVolumesetCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volumeset", []string{
			"[(--name <volumeset name> | -n <volumeset name>)] [(--attributes <key>=<value>,... | -a <key>=<value>,...)]  [(--description <description> | -d <description>)] (<Volumeset ID> | <Volumeset Path>)",
		}),
		Short: "Update the volumeset metadata. This command udpates the name and attributes of the volumeset. To update a key-value pair pass it with --attributes with old key name and new value. Keys in attributes are case-insensitive. The argument passed could be Volumeset ID or the Volumeset Name.",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var nameF string
			nameF, err = cmd.Flags().GetString("name")
			if err != nil {
				return err
			}

			var attributesF string
			attributesF, err = cmd.Flags().GetString("attributes")
			if err != nil {
				return err
			}

			var descriptionF string
			descriptionF, err = cmd.Flags().GetString("description")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerUpdateVolumeset(
				nameF,
				attributesF,
				descriptionF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("name", "n", "", "Update or add a heirarchical or flat name for the volumeset. This name can be used for doing lookups for the volumesets.")
	cmd.Flags().StringP("attributes", "a", "", "Update or add attributes to existing key-value pairs.")
	cmd.Flags().StringP("description", "d", "", "Update the description for the volumeset")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newUpdateSnapshotCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("snapshot", []string{
			"[(--name <snapshot name> | -n <snapshot name>)] [(--attributes <key>=<value>,... | -a <key>=<value>,...)]  [(--description <description> | -d <description>)] <Snapshot ID>",
		}),
		Short: "Update the snapshot metadata. This command udpates the name and attributes of the snapshot. To update a key-value pair pass it with --attributes with old key name and new value. Keys in attributes are case-insensitive.",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var nameF string
			nameF, err = cmd.Flags().GetString("name")
			if err != nil {
				return err
			}

			var attributesF string
			attributesF, err = cmd.Flags().GetString("attributes")
			if err != nil {
				return err
			}

			var descriptionF string
			descriptionF, err = cmd.Flags().GetString("description")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerUpdateSnapshot(
				nameF,
				attributesF,
				descriptionF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("name", "n", "", "Update or add the snapshot name.")
	cmd.Flags().StringP("attributes", "a", "", "Update or add attributes to existing key-value pairs.")
	cmd.Flags().StringP("description", "d", "", "Update the description for the snapshot")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newUpdateBranchCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("branch", []string{
			"(--volumeset <Volumeset Path | Volumeset ID> | -v <Volumeset Path | volumeset ID>) <Current branch name> <New branch name>",
		}),
		Short: "Renames the branch name ",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var volumesetF string
			volumesetF, err = cmd.Flags().GetString("volumeset")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerUpdateBranch(
				volumesetF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("volumeset", "v", "", "Volumeset identifier where the branch exists")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newUpdateVolumeCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volume", []string{
			"(--attributes <key>=<value>,... | -a <key>=<value>,...) <Volume ID>",
		}),
		Short: "Update the volume metadata. This command udpates the attributes of the volumes. To update a key-value pair pass it with --attributes with old key name and new value. Keys in attributes are case-insensitive.",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var attributesF string
			attributesF, err = cmd.Flags().GetString("attributes")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerUpdateVolume(
				attributesF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("attributes", "a", "", "Update or add attributes to existing key-value pairs.")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newDpcliRemoveCmd(h handlerIface) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "remove",
		Short: "This command can be used to remove data and metadata of different objects based on the subcommands.",
	}

	cmd.AddCommand(newRemoveVolumesetCmd(h))
	cmd.AddCommand(newRemoveSnapshotCmd(h))
	cmd.AddCommand(newRemoveBranchCmd(h))
	cmd.AddCommand(newRemoveVolumeCmd(h))

	return cmd
}

func newRemoveVolumesetCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volumeset", []string{
			"<Volumeset-ID | Volumeset Path>...",
		}),
		Short: "Remove volumeset passed as argument. This will remove all the storage used by the snapshot and volumes of the volumeset. The metadata will still remain and can be fetched from the Volume Hub.",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var s []byte
			s, err = h.handlerRemoveVolumeset(
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newRemoveSnapshotCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("snapshot", []string{
			"<Snapshot ID>...",
		}),
		Short: "Removes the snapshots passed as arguments. This will free the storage used by the snapshot. The snapshot metadata will remain and can be fetched from the Volume Hub.",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var s []byte
			s, err = h.handlerRemoveSnapshot(
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newRemoveBranchCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("branch", []string{
			"<Branch>...",
		}),
		Short: "Remove the branch from the local metadata store. This will remove all the snapshots starting from the tip to the point where the branch was created. The starting point for the branch could be split from another branch or the root.",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var volumesetF string
			volumesetF, err = cmd.Flags().GetString("volumeset")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerRemoveBranch(
				volumesetF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("volumeset", "v", "", "volumeset id where the branch exists")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newRemoveVolumeCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volume", []string{
			"<Volume-ID>...",
		}),
		Short: "Removes volumes passed as arguments. This will remove the volume data and the metadata. ",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var s []byte
			s, err = h.handlerRemoveVolume(
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newDpcliPushCmd(h handlerIface) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "push",
		Short: "Push snapshots to the Volume Hub",
	}

	cmd.AddCommand(newPushVolumesetCmd(h))
	cmd.AddCommand(newPushSnapshotCmd(h))

	return cmd
}

func newPushVolumesetCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volumeset", []string{
			"[(--vhub <vhub root url> | -H <vhub root url>)] [(--tokenfile <token-file> | -t <token-file>)] <volumeset id>...",
		}),
		Short: "Push all the snapshots for the given volumeset to Volume Hub",
		Long:  `The snapshots are pushed to the Volume Hub for the given volumeset. It will only push those snapshots that are not available on the Volume Hub and each increamental snapshot pushed is a diff. Before running this command the meta-data should be synchronized. See 'dpcli help sync volumeset'. In case of divergence this command will throw an error. This operation can take a considerable time based on the number of snapshots, their size and network bandwidth.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var vhubF string
			vhubF, err = cmd.Flags().GetString("vhub")
			if err != nil {
				return err
			}

			var tokenfileF string
			tokenfileF, err = cmd.Flags().GetString("tokenfile")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerPushVolumeset(
				vhubF,
				tokenfileF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("vhub", "H", "", "Volume Hub URL")
	cmd.Flags().StringP("tokenfile", "t", "", "Volume Hub token")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newPushSnapshotCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("snapshot", []string{
			"[(--vhub <vhub root url> | -H <vhub root url>)] [(--tokenfile <token-file> | -t <token-file>)] <snapshot id>...",
		}),
		Short: "Push the snapshot to the Volume Hub",
		Long:  `The snapshot is pushed to the Volume Hub. It will only push  snapshots if its not available on the Volume Hub and each increamental snapshot pushed is a diff. Before running this command the meta-data should be synchronized. See 'dpcli help sync volumeset'. In case of divergence this command will throw an error. This operation can take a considerable time based on the size and network bandwidth.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var vhubF string
			vhubF, err = cmd.Flags().GetString("vhub")
			if err != nil {
				return err
			}

			var tokenfileF string
			tokenfileF, err = cmd.Flags().GetString("tokenfile")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerPushSnapshot(
				vhubF,
				tokenfileF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("vhub", "H", "", "Volume Hub URL")
	cmd.Flags().StringP("tokenfile", "t", "", "Volume Hub token")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newDpcliPullCmd(h handlerIface) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "pull",
		Short: "Push snapshots to the Volume Hub",
	}

	cmd.AddCommand(newPullVolumesetCmd(h))
	cmd.AddCommand(newPullSnapshotCmd(h))

	return cmd
}

func newPullVolumesetCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volumeset", []string{
			"[(--vhub <vhub root url> | -H <vhub root url>)] [(--tokenfile <token-file> | -t <token-file>)] <volumeset id>...",
		}),
		Short: "Pull all the snapshots for the volumeset from Volume Hub",
		Long:  `The snapshots are pulled from the Volume Hub for the given volumeset. It will only pull those snapshots that are not available locally and each increamental snapshot pulled is a diff. See 'dpcli help sync volumeset'. In case of divergence this command will throw an error. This operation can take a considerable time based on the number of snapshots, their size and network bandwidth. Also make sure you have enough storage available on the client.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var vhubF string
			vhubF, err = cmd.Flags().GetString("vhub")
			if err != nil {
				return err
			}

			var tokenfileF string
			tokenfileF, err = cmd.Flags().GetString("tokenfile")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerPullVolumeset(
				vhubF,
				tokenfileF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("vhub", "H", "", "Volume Hub URL")
	cmd.Flags().StringP("tokenfile", "t", "", "Volume Hub token")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newPullSnapshotCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("snapshot", []string{
			"[(--vhub <vhub root url> | -H <vhub root url>)] [(--tokenfile <token-file> | -t <token-file>)] <snapshot id>...",
		}),
		Short: "Pull the snapshot from the Volume Hub",
		Long:  `The snapshots are pulled from the Volume Hub. It will only pull snapshots if it is not available locally and each increamental snapshot pulled is a diff. Before running this command the meta-data should be synchronized. See 'dpcli help sync volumeset'. In case of any divergence this command will throw an error. This operation can take a considerable time based on the size and network bandwidth. Also make sure you have enough storage available on the client.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var vhubF string
			vhubF, err = cmd.Flags().GetString("vhub")
			if err != nil {
				return err
			}

			var tokenfileF string
			tokenfileF, err = cmd.Flags().GetString("tokenfile")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerPullSnapshot(
				vhubF,
				tokenfileF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("vhub", "H", "", "Volume Hub URL")
	cmd.Flags().StringP("tokenfile", "t", "", "Volume Hub token")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newDpcliSyncCmd(h handlerIface) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "sync",
		Short: "Synchronize volumeset meta-data with the Volume Hub",
	}

	cmd.AddCommand(newSyncVolumesetCmd(h))

	return cmd
}

func newSyncVolumesetCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volumeset", []string{
			"[(--vhub <vhub root url> | -H <vhub root url>)] [(--tokenfile <token-file> | -t <token-file>)] <volumeset id>...",
		}),
		Short: "Synchronize the given volumeset with the Volume Hub",
		Long:  `A two-way sync is performed. This will udpate the meta-data between the local store and the Volume Hub store. Before running this command the meta-data should be synchronized. In case of divergence this command will fail. Divergence could be caused if two or more clients are trying to update a single volumeset.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var vhubF string
			vhubF, err = cmd.Flags().GetString("vhub")
			if err != nil {
				return err
			}

			var tokenfileF string
			tokenfileF, err = cmd.Flags().GetString("tokenfile")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerSyncVolumeset(
				vhubF,
				tokenfileF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().StringP("vhub", "H", "", "Volume Hub URL")
	cmd.Flags().StringP("tokenfile", "t", "", "Volume Hub token")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newDpcliInitCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize dpcli to setup local meta-data store",
		Long:  `Initializes local meta-data store on the client. This command is used only once when setting up the meta-data store. To destory and recreate the store use --force option. This is an destructive operation and anything that is not synchronized and pushed to Volume Hub will be lost. The snapshots or the volume are not removed after using this command.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var forceF bool
			forceF, err = cmd.Flags().GetBool("force")
			if err != nil {
				return err
			}

			var zpoolF string
			zpoolF, err = cmd.Flags().GetString("zpool")
			if err != nil {
				return err
			}

			var s []byte
			s, err = h.handlerDpcliInit(
				forceF,
				zpoolF,
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().BoolP("force", "f", false, "Force to create a new meta-data store if it already exists. This is a desctructive operation and can not be reverted.")
	cmd.Flags().StringP("zpool", "z", "chq", "ZPOOL name to be used for the dpcli.")

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newDpcliSetCmd(h handlerIface) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "set",
		Short: "Set some commonly used key-value pairs in local meta-data store",
	}

	cmd.AddCommand(newSetVolumehubCmd(h))
	cmd.AddCommand(newSetTokenfileCmd(h))

	return cmd
}

func newSetVolumehubCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volumehub", []string{
			"<Volume Hub URL>",
		}),
		Short: "Set the default value for Volume Hub URL",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var s []byte
			s, err = h.handlerSetVolumehub(
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newSetTokenfileCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("tokenfile", []string{
			"<Volume Hub token file>",
		}),
		Short: "Set the default value for Volume Hub token file",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var s []byte
			s, err = h.handlerSetTokenfile(
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newDpcliGetCmd(h handlerIface) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "get",
		Short: "Get some commonly used key-value pairs in local meta-data store",
	}

	cmd.AddCommand(newGetVolumehubCmd(h))
	cmd.AddCommand(newGetTokenfileCmd(h))

	return cmd
}

func newGetVolumehubCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("volumehub", []string{
			"<Volume Hub URL>",
		}),
		Short: "Get the default value Volume Hub URL",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var s []byte
			s, err = h.handlerGetVolumehub(
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

func newGetTokenfileCmd(h handlerIface) *cobra.Command {
	var jsonF = false

	var cmd = &cobra.Command{
		Use: getMultiUseLine("tokenfile", []string{
			"<Volume Hub token file>",
		}),
		Short: "Get the default value for Volume Hub token file",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			var s []byte
			s, err = h.handlerGetTokenfile(
				args,
				jsonF,
			)

			if jsonF {
				cmd.Print(string(s[:]))
			} else {
				var js JSONOp
				errJ := json.Unmarshal(s, &js)
				if errJ != nil {
					fmt.Fprintf(os.Stderr, "internal error: %s\n", errJ)
					os.Exit(1)
				}

				cmd.Print(js.Str)
				cmd.Print(genTable(js.Tab))
			}

			if err != nil && strings.HasPrefix(err.Error(), "internal error:") {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}

			return err
		},
	}

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")

	return cmd
}

type handlerIface interface {
	handlerCreateVolumeset(attributes string, description string, args []string, jsonF bool) ([]byte, error)
	handlerCreateSnapshot(branch string, volume string, attributes string, description string, args []string, jsonF bool) ([]byte, error)
	handlerCreateVolume(volumeset string, branch string, snapshot string, attributes string, args []string, jsonF bool) ([]byte, error)
	handlerShowVolumeset(args []string, jsonF bool) ([]byte, error)
	handlerShowSnapshot(volumeset string, args []string, jsonF bool) ([]byte, error)
	handlerShowBranch(volumeset string, args []string, jsonF bool) ([]byte, error)
	handlerShowVolume(volumeset string, args []string, jsonF bool) ([]byte, error)
	handlerUpdateVolumeset(name string, attributes string, description string, args []string, jsonF bool) ([]byte, error)
	handlerUpdateSnapshot(name string, attributes string, description string, args []string, jsonF bool) ([]byte, error)
	handlerUpdateBranch(volumeset string, args []string, jsonF bool) ([]byte, error)
	handlerUpdateVolume(attributes string, args []string, jsonF bool) ([]byte, error)
	handlerRemoveVolumeset(args []string, jsonF bool) ([]byte, error)
	handlerRemoveSnapshot(args []string, jsonF bool) ([]byte, error)
	handlerRemoveBranch(volumeset string, args []string, jsonF bool) ([]byte, error)
	handlerRemoveVolume(args []string, jsonF bool) ([]byte, error)
	handlerPushVolumeset(vhub string, tokenfile string, args []string, jsonF bool) ([]byte, error)
	handlerPushSnapshot(vhub string, tokenfile string, args []string, jsonF bool) ([]byte, error)
	handlerPullVolumeset(vhub string, tokenfile string, args []string, jsonF bool) ([]byte, error)
	handlerPullSnapshot(vhub string, tokenfile string, args []string, jsonF bool) ([]byte, error)
	handlerSyncVolumeset(vhub string, tokenfile string, args []string, jsonF bool) ([]byte, error)
	handlerDpcliInit(force bool, zpool string, args []string, jsonF bool) ([]byte, error)
	handlerSetVolumehub(args []string, jsonF bool) ([]byte, error)
	handlerSetTokenfile(args []string, jsonF bool) ([]byte, error)
	handlerGetVolumehub(args []string, jsonF bool) ([]byte, error)
	handlerGetTokenfile(args []string, jsonF bool) ([]byte, error)
}
