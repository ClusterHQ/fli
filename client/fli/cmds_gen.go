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
 * CODE GENERATED AUTOMATICALLY WITH github.com/ClusterHQ/fli/client/cmd/cmdgen
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */

package fli

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

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
				fmt.Fprint(wr, "\n")
			}
		}
	}

	// flush to stdout
	wr.Flush()

	return string(buf.Bytes()[:])
}

func displayOutput(cmd *cobra.Command, out Result) {
	json, err := cmd.Flags().GetBool("json")
	if err != nil {
		cmd.Println(err)
		os.Exit(1)
	}

	if json {
		cmd.Print(out.JSON())
	} else {
		cmd.Print(out.String())
	}
}

// handleError handles errors returned by handler implementations
func handleError(cmd *cobra.Command, e error) {
	if e == nil {
		return
	}

	cmd.Println(e.Error())

	switch e.(type) {
	// These errors need the command help to be displayed
	case ErrInvalidArgs:
		cmd.Usage()
		break
	case ErrMissingFlag:
		cmd.Usage()
		break
	default:
		break
	}

	os.Exit(1)
}

func newLogger() (*log.Logger, error) {
	logFile := filepath.Join(LogDir, CmdLogFilename)

	os.MkdirAll(LogDir, (os.ModeDir | 0755))

	fp, err := os.OpenFile(logFile, (os.O_CREATE | os.O_WRONLY | os.O_APPEND), 0666)
	if err != nil {
		return nil, err
	}

	return log.New(io.MultiWriter(fp), "", (log.Ldate | log.Ltime)), nil
}

func init() {
	cobra.AddTemplateFunc("multiUseLine", multiUseLine)
}

type sortedCommands []*cobra.Command

func (c sortedCommands) Len() int           { return len(c) }
func (c sortedCommands) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c sortedCommands) Less(i, j int) bool { return c[i].Name() < c[j].Name() }

func newFliCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "fli",
		Short: "fli",
		Long: `fli is a command line utility that manages volumesets, volumes, snapshots and branches along with their metadata.
fli synchronizes metadata with FlockerHub and can push and pull snapshots of volumes from/to FlockerHub.`,
	}

	cmd.PersistentFlags().BoolP(
		"json",
		"",
		false,
		"Output using JSON format")

	var outputF = ""
	var complCmd = &cobra.Command{
		Use:   getMultiUseLine("completion", []string{"(--output <filepath> | -o <filepath>)"}),
		Short: "Generates bash completion file for fli command",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Parent().GenBashCompletionFile(outputF)
		},
	}
	complCmd.Flags().StringVarP(&outputF, "output", "o", "", "Generates bash completions file for fli")

	sortedCmds := sortedCommands{
		newCloneCmd(ctx, h),
		newConfigCmd(ctx, h),
		newCreateCmd(ctx, h),
		newInitCmd(ctx, h),
		newListCmd(ctx, h),
		newPullCmd(ctx, h),
		newPushCmd(ctx, h),
		newRemoveCmd(ctx, h),
		newSetupCmd(ctx, h),
		newSnapshotCmd(ctx, h),
		newSyncCmd(ctx, h),
		newFetchCmd(ctx, h),
		newUpdateCmd(ctx, h),
		newVersionCmd(ctx, h),
		newInfoCmd(ctx, h),
		newDiagnosticsCmd(ctx, h),
		complCmd,
	}

	sort.Sort(sortedCmds)
	for _, c := range sortedCmds {
		cmd.AddCommand(c)
	}

	return cmd
}

func newCloneCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("clone", []string{
			"[OPTIONS] SNAPSHOT [VOLUME-NAME]",
			"[OPTIONS] VOLUMESET:SNAPSHOT [VOLUME-NAME]",
			"[OPTIONS] BRANCH [VOLUME-NAME]",
			"[OPTIONS] VOLUMESET:BRANCH [VOLUME-NAME]",
		}),
		Short: "Creates a copy of a volume from a given snapshot or branch",
		Long: `A volume is cloned from a snapshot of another volume. A branch can be used to clone a volume. This volume is cloned from a snapshot that is the tip of the branch.
If more than one matching result for the snapshot is found then it is treated as ambiguous output and all the matching results are displayed. The volumeset, snapshot and branch could be name or uuid.
`,
		Example: `The following example explains how to clone a volume named 'newVolumeName' from a snapshot 'exampleSnapshotName' that belongs to volumeset 'exampleVolSetName'

    $ fli clone exampleVolSetName:exampleSnapshotName newVolumeName --attributes For=Test,Ref=HelpCommand
`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err            error
				attributesFlag string
				fullFlag       bool
			)

			attributesFlag, err = cmd.Flags().GetString("attributes")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			fullFlag, err = cmd.Flags().GetBool("full")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli clone --attributes '%v' --full '%v' '%v'",
				attributesFlag,
				fullFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli clone --attributes '%v' --full '%v' '%v'",
				attributesFlag,
				fullFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Clone(
				attributesFlag,
				fullFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	attributesDefVal := ""
	if v := ctx.Value(attributesKey); v != nil {
		attributesDefVal = ctx.Value(attributesKey).(string)
	}

	cmd.Flags().StringP(
		"attributes",
		"a",
		attributesDefVal,
		"A comma separated list of a key-value pairs(ex: userKey1=userVal1,userKey2=userVal2)")

	cmd.Flags().BoolP(
		"full",
		"",
		false,
		"Report full UUIDs for objects instead of short UUIDs")

	return cmd
}

func newConfigCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("config", []string{
			"[OPTIONS]",
		}),
		Short: "Read and update the commonly used parameters",
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				urlFlag     string
				tokenFlag   string
				offlineFlag bool
			)

			urlFlag, err = cmd.Flags().GetString("url")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			tokenFlag, err = cmd.Flags().GetString("token")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			offlineFlag, err = cmd.Flags().GetBool("offline")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli config --url '%v' --token '%v' --offline '%v' '%v'",
				urlFlag,
				tokenFlag,
				offlineFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli config --url '%v' --token '%v' --offline '%v' '%v'",
				urlFlag,
				tokenFlag,
				offlineFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Config(
				urlFlag,
				tokenFlag,
				offlineFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	urlDefVal := ""
	if v := ctx.Value(urlKey); v != nil {
		urlDefVal = ctx.Value(urlKey).(string)
	}

	cmd.Flags().StringP(
		"url",
		"u",
		urlDefVal,
		"FlockerHub URL (Example: https://flockerhub.com or http://flockerhub.com)")

	tokenDefVal := ""
	if v := ctx.Value(tokenKey); v != nil {
		tokenDefVal = ctx.Value(tokenKey).(string)
	}

	cmd.Flags().StringP(
		"token",
		"t",
		tokenDefVal,
		"Absolute path of the authentication token file")

	cmd.Flags().BoolP(
		"offline",
		"",
		false,
		"Use this option to avoid URL and token validate with FlockerHub")

	return cmd
}

func newCreateCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("create", []string{
			"[OPTIONS] VOLUMESET [VOLUME-NAME]",
		}),
		Short: "Create an empty volume",
		Long: `Creates an empty volume for a given volumeset. If the volumeset does not exist then a new volumeset with the given name is created.
`,
		Example: `The following example explains how to create an empty volume for a given volumeset.

    $ fli create exampleVolSetName newEmptyVolName --attributes For=Example,Ref=HelpCommand
`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err            error
				attributesFlag string
				fullFlag       bool
			)

			attributesFlag, err = cmd.Flags().GetString("attributes")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			fullFlag, err = cmd.Flags().GetBool("full")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli create --attributes '%v' --full '%v' '%v'",
				attributesFlag,
				fullFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli create --attributes '%v' --full '%v' '%v'",
				attributesFlag,
				fullFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Create(
				attributesFlag,
				fullFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	attributesDefVal := ""
	if v := ctx.Value(attributesKey); v != nil {
		attributesDefVal = ctx.Value(attributesKey).(string)
	}

	cmd.Flags().StringP(
		"attributes",
		"a",
		attributesDefVal,
		"A comma separated list of a key-value pairs(ex: userKey1=userVal1,userKey2=userVal2)")

	cmd.Flags().BoolP(
		"full",
		"",
		false,
		"Report full UUIDs for objects instead of short UUIDs")

	return cmd
}

func newInitCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("init", []string{
			"[OPTIONS] [VOLUMESET-NAME]",
		}),
		Short: "Initialize a new volumeset",
		Example: `The following example explains how to initialize a new volumeset.

    $ fli init newVolSetName
`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err             error
				attributesFlag  string
				descriptionFlag string
			)

			attributesFlag, err = cmd.Flags().GetString("attributes")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			descriptionFlag, err = cmd.Flags().GetString("description")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli init --attributes '%v' --description '%v' '%v'",
				attributesFlag,
				descriptionFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli init --attributes '%v' --description '%v' '%v'",
				attributesFlag,
				descriptionFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Init(
				attributesFlag,
				descriptionFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	attributesDefVal := ""
	if v := ctx.Value(attributesKey); v != nil {
		attributesDefVal = ctx.Value(attributesKey).(string)
	}

	cmd.Flags().StringP(
		"attributes",
		"a",
		attributesDefVal,
		"A comma separated list of a key-value pairs(ex: userKey1=userVal1,userKey2=userVal2)")

	descriptionDefVal := ""
	if v := ctx.Value(descriptionKey); v != nil {
		descriptionDefVal = ctx.Value(descriptionKey).(string)
	}

	cmd.Flags().StringP(
		"description",
		"d",
		descriptionDefVal,
		"A short description for the volumeset")

	return cmd
}

func newListCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("list", []string{
			"[OPTIONS] VOLUMESET",
			"[OPTIONS] VOLUMESET:SNAPSHOT",
			"[OPTIONS] VOLUMESET:BRANCH",
			"[OPTIONS] VOLUMESET:VOLUME",
		}),
		Short: "Reports all the objects",
		Aliases: []string{
			"show",

			"search",
		},
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err          error
				allFlag      bool
				volumeFlag   bool
				snapshotFlag bool
				branchFlag   bool
				fullFlag     bool
			)

			allFlag, err = cmd.Flags().GetBool("all")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			volumeFlag, err = cmd.Flags().GetBool("volume")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			snapshotFlag, err = cmd.Flags().GetBool("snapshot")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			branchFlag, err = cmd.Flags().GetBool("branch")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			fullFlag, err = cmd.Flags().GetBool("full")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli list --all '%v' --volume '%v' --snapshot '%v' --branch '%v' --full '%v' '%v'",
				allFlag,
				volumeFlag,
				snapshotFlag,
				branchFlag,
				fullFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli list --all '%v' --volume '%v' --snapshot '%v' --branch '%v' --full '%v' '%v'",
				allFlag,
				volumeFlag,
				snapshotFlag,
				branchFlag,
				fullFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.List(
				allFlag,
				volumeFlag,
				snapshotFlag,
				branchFlag,
				fullFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	cmd.Flags().BoolP(
		"all",
		"a",
		false,
		"Reports the complete hierarchy of the volumeset. If an argument is passed then the hierarchy of the object is displayed.")

	cmd.Flags().BoolP(
		"volume",
		"v",
		false,
		"Reports only the volumes")

	cmd.Flags().BoolP(
		"snapshot",
		"s",
		false,
		"Reports only the snapshots of the volume")

	cmd.Flags().BoolP(
		"branch",
		"b",
		false,
		"Reports only the branches")

	cmd.Flags().BoolP(
		"full",
		"",
		false,
		"Report full UUIDs for objects instead of short UUIDs")

	return cmd
}

func newPullCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("pull", []string{
			"[OPTIONS] VOLUMESET",
			"[OPTIONS] VOLUMESET:SNAPSHOT",
		}),
		Short: "Pull a single snapshot or all snapshots of volume in a volumeset from FlockerHub",
		Long: `Pulls a volumeset or snapshot of volume from FlockerHub. This command needs a FlockerHub URL and an authentication token that can be downloaded from FlockerHub for the user.
The FlockerHub URL and token filepath can be set one time using 'fli config' for all the commands that need this options.
`,
		Example: `The following example explains how to pull all snapshots of a volume

    $ fli pull exampleVolSetName --url https://example.flockerhub.com --token /home/demoUser/auth.token

The following example explains how to pull a single snapshot of a volume

    $ fli pull exampleVolSetName:exampleSnapName --url https://example.flockerhub.com --token /home/demoUser/auth.token
`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err       error
				urlFlag   string
				tokenFlag string
				fullFlag  bool
			)

			urlFlag, err = cmd.Flags().GetString("url")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			tokenFlag, err = cmd.Flags().GetString("token")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			fullFlag, err = cmd.Flags().GetBool("full")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli pull --url '%v' --token '%v' --full '%v' '%v'",
				urlFlag,
				tokenFlag,
				fullFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli pull --url '%v' --token '%v' --full '%v' '%v'",
				urlFlag,
				tokenFlag,
				fullFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Pull(
				urlFlag,
				tokenFlag,
				fullFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	urlDefVal := ""
	if v := ctx.Value(urlKey); v != nil {
		urlDefVal = ctx.Value(urlKey).(string)
	}

	cmd.Flags().StringP(
		"url",
		"u",
		urlDefVal,
		"FlockerHub URL (Example: https://flockerhub.com or http://flockerhub.com)")

	tokenDefVal := ""
	if v := ctx.Value(tokenKey); v != nil {
		tokenDefVal = ctx.Value(tokenKey).(string)
	}

	cmd.Flags().StringP(
		"token",
		"t",
		tokenDefVal,
		"Absolute path of the authentication token file")

	cmd.Flags().BoolP(
		"full",
		"",
		false,
		"Report full UUIDs for objects instead of short UUIDs")

	return cmd
}

func newPushCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("push", []string{
			"[OPTIONS] VOLUMESET",
			"[OPTIONS] VOLUMESET:SNAPSHOT",
		}),
		Short: "Push snapshot of a volume to FlockerHub",
		Long: `Pushes snapshot of a volume to FlockerHub. This command needs a FlockerHub URL and an authentication token that can be downloaded from FlockerHub for the user. If you specify the volumeset instead of the snapshot then all the snapshots are pushed.
The FlockerHub URL and token filepath can be set one time using 'fli config' for all the commands that need this options.
`,
		Example: `The following example explains how to push all snapshots of a volume

    $ fli push exampleVolSetName --url https://example.flockerhub.com --token /home/demoUser/auth.token

The following example explains how to push a single snapshot of a volume

    $ fli push exampleVolSetName:exampleSnapName --url https://example.flockerhub.com --token /home/demoUser/auth.token
`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err       error
				urlFlag   string
				tokenFlag string
				fullFlag  bool
			)

			urlFlag, err = cmd.Flags().GetString("url")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			tokenFlag, err = cmd.Flags().GetString("token")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			fullFlag, err = cmd.Flags().GetBool("full")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli push --url '%v' --token '%v' --full '%v' '%v'",
				urlFlag,
				tokenFlag,
				fullFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli push --url '%v' --token '%v' --full '%v' '%v'",
				urlFlag,
				tokenFlag,
				fullFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Push(
				urlFlag,
				tokenFlag,
				fullFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	urlDefVal := ""
	if v := ctx.Value(urlKey); v != nil {
		urlDefVal = ctx.Value(urlKey).(string)
	}

	cmd.Flags().StringP(
		"url",
		"u",
		urlDefVal,
		"FlockerHub URL (Example: https://flockerhub.com or http://flockerhub.com)")

	tokenDefVal := ""
	if v := ctx.Value(tokenKey); v != nil {
		tokenDefVal = ctx.Value(tokenKey).(string)
	}

	cmd.Flags().StringP(
		"token",
		"t",
		tokenDefVal,
		"Absolute path of the authentication token file")

	cmd.Flags().BoolP(
		"full",
		"",
		false,
		"Report full UUIDs for objects instead of short UUIDs")

	return cmd
}

func newRemoveCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("remove", []string{
			"[OPTIONS] VOLUMESET",
			"[OPTIONS] VOLUMESET:SNAPSHOT",
			"[OPTIONS] VOLUMESET:BRANCH",
			"[OPTIONS] VOLUMESET:VOLUME",
		}),
		Short: "Removes the object based on the name",
		Aliases: []string{
			"rm",

			"delete",
		},
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err      error
				fullFlag bool
			)

			fullFlag, err = cmd.Flags().GetBool("full")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli remove --full '%v' '%v'",
				fullFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli remove --full '%v' '%v'",
				fullFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Remove(
				fullFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	cmd.Flags().BoolP(
		"full",
		"",
		false,
		"Report full UUIDs for objects instead of short UUIDs")

	return cmd
}

func newSetupCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("setup", []string{
			"[OPTIONS]",
		}),
		Short: "Sets up the fli metadata store and storage hooks",
		Long: `Setup is used for the first time and it sets up the metadata stores for using fli. Setup can also be used to cleanup and setup a new metadata store. The cleanup will remove all the metadata associated with the storage objects and cannot be recovered unless it has been synchronized and pushed to FlockerHub.
`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err       error
				zpoolFlag string
				forceFlag bool
			)

			zpoolFlag, err = cmd.Flags().GetString("zpool")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			forceFlag, err = cmd.Flags().GetBool("force")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli setup --zpool '%v' --force '%v' '%v'",
				zpoolFlag,
				forceFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli setup --zpool '%v' --force '%v' '%v'",
				zpoolFlag,
				forceFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Setup(
				zpoolFlag,
				forceFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	zpoolDefVal := ""
	if v := ctx.Value(zpoolKey); v != nil {
		zpoolDefVal = ctx.Value(zpoolKey).(string)
	}

	cmd.Flags().StringP(
		"zpool",
		"z",
		zpoolDefVal,
		"ZFS zpool name that is to be used by fli")

	cmd.Flags().BoolP(
		"force",
		"f",
		false,
		"Force remove previous metadata files. This will remove your previously created objects if they are not synced with FlockerHub")

	return cmd
}

func newSnapshotCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("snapshot", []string{
			"[OPTIONS] VOLUMESET:VOLUME [SNAPSHOT-NAME]",
		}),
		Short: "Takes a snapshot of a volume and associates it to a branch",
		Long: `Snapshot command captures a consistent view of a volume. Each snapshot is associated to a branch that can grow when more snapshots of the volumes are taken. A snapshot of a volume can be assigned attributes (--attributes) and descriptions (--description).
A branch can grow or fork into another branch. A snapshot of a volume can be taken with or without a name and the name should be unique inside a volumeset.
VOLUMESET and VOLUME could be a name or uuid.
`,
		Example: `The following example explains how to snapshot of volume named 'exampleVolName' from a volumeset 'exampleVolSetName' and name it 'newSnapshotName'

    $ fli snapshot exampleVolSetName:exampleVolName newSnapshotName --attributes For=FliExample,Ref=HelpCommand --description 'This is an example of how to take a snapshot of volume'
`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err             error
				branchFlag      string
				newbranchFlag   bool
				attributesFlag  string
				descriptionFlag string
				fullFlag        bool
			)

			branchFlag, err = cmd.Flags().GetString("branch")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			newbranchFlag, err = cmd.Flags().GetBool("new-branch")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			attributesFlag, err = cmd.Flags().GetString("attributes")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			descriptionFlag, err = cmd.Flags().GetString("description")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			fullFlag, err = cmd.Flags().GetBool("full")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli snapshot --branch '%v' --new-branch '%v' --attributes '%v' --description '%v' --full '%v' '%v'",
				branchFlag,
				newbranchFlag,
				attributesFlag,
				descriptionFlag,
				fullFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli snapshot --branch '%v' --new-branch '%v' --attributes '%v' --description '%v' --full '%v' '%v'",
				branchFlag,
				newbranchFlag,
				attributesFlag,
				descriptionFlag,
				fullFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Snapshot(
				branchFlag,
				newbranchFlag,
				attributesFlag,
				descriptionFlag,
				fullFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	branchDefVal := ""
	if v := ctx.Value(branchKey); v != nil {
		branchDefVal = ctx.Value(branchKey).(string)
	}

	cmd.Flags().StringP(
		"branch",
		"b",
		branchDefVal,
		"Branch name for the snapshot of volume")

	cmd.Flags().BoolP(
		"new-branch",
		"",
		false,
		"Create a new branch without a name for the snapshot of volume")

	attributesDefVal := ""
	if v := ctx.Value(attributesKey); v != nil {
		attributesDefVal = ctx.Value(attributesKey).(string)
	}

	cmd.Flags().StringP(
		"attributes",
		"a",
		attributesDefVal,
		"A comma separated list of a key-value pairs(ex: userKey1=userVal1,userKey2=userVal2)")

	descriptionDefVal := ""
	if v := ctx.Value(descriptionKey); v != nil {
		descriptionDefVal = ctx.Value(descriptionKey).(string)
	}

	cmd.Flags().StringP(
		"description",
		"d",
		descriptionDefVal,
		"A short description of the snapshot of the volume")

	cmd.Flags().BoolP(
		"full",
		"",
		false,
		"Report full UUIDs for objects instead of short UUIDs")

	return cmd
}

func newSyncCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("sync", []string{
			"[OPTIONS] VOLUMESET",
		}),
		Short: "Synchronize the metadata of volumeset with the FlockerHub",
		Long: `Synchronizes the metadata with FlockerHub. This command needs a FlockerHub URL and an authentication token that can be downloaded from the FlockerHub for a given user.
The FlockerHub URL and token filepath can be set one time using 'fli config' for all the commands that need this options.
`,
		Example: `The following example explains how to synchronize a volumeset with the FlockerHub

    $ fli sync exampleVolSetName --url https://example.flockerhub.com --token /home/demoUser/auth.token
`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err       error
				urlFlag   string
				tokenFlag string
				allFlag   bool
				fullFlag  bool
			)

			urlFlag, err = cmd.Flags().GetString("url")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			tokenFlag, err = cmd.Flags().GetString("token")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			allFlag, err = cmd.Flags().GetBool("all")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			fullFlag, err = cmd.Flags().GetBool("full")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli sync --url '%v' --token '%v' --all '%v' --full '%v' '%v'",
				urlFlag,
				tokenFlag,
				allFlag,
				fullFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli sync --url '%v' --token '%v' --all '%v' --full '%v' '%v'",
				urlFlag,
				tokenFlag,
				allFlag,
				fullFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Sync(
				urlFlag,
				tokenFlag,
				allFlag,
				fullFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	urlDefVal := ""
	if v := ctx.Value(urlKey); v != nil {
		urlDefVal = ctx.Value(urlKey).(string)
	}

	cmd.Flags().StringP(
		"url",
		"u",
		urlDefVal,
		"FlockerHub URL (Example: https://flockerhub.com or http://flockerhub.com)")

	tokenDefVal := ""
	if v := ctx.Value(tokenKey); v != nil {
		tokenDefVal = ctx.Value(tokenKey).(string)
	}

	cmd.Flags().StringP(
		"token",
		"t",
		tokenDefVal,
		"Absolute path of the authentication token file")

	cmd.Flags().BoolP(
		"all",
		"a",
		false,
		"Sync all volumesets available locally")

	cmd.Flags().BoolP(
		"full",
		"",
		false,
		"Report full UUIDs for objects instead of short UUIDs")

	return cmd
}

func newFetchCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("fetch", []string{
			"[OPTIONS] VOLUMESET",
		}),
		Short: "Fetch the metadata of volumeset from the FlockerHub",
		Long: `Fetches the metadata from FlockerHub. This command needs a FlockerHub URL and an authentication token that can be downloaded from the FlockerHub for a given user.
The FlockerHub URL and token filepath can be set one time using 'fli config' for all the commands that need this options.
`,
		Example: `The following example explains how to fetch a volumeset metadata from the FlockerHub

    $ fli fetch exampleVolSetName --url https://example.flockerhub.com --token /home/demoUser/auth.token
`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err       error
				urlFlag   string
				tokenFlag string
				allFlag   bool
				fullFlag  bool
			)

			urlFlag, err = cmd.Flags().GetString("url")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			tokenFlag, err = cmd.Flags().GetString("token")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			allFlag, err = cmd.Flags().GetBool("all")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			fullFlag, err = cmd.Flags().GetBool("full")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli fetch --url '%v' --token '%v' --all '%v' --full '%v' '%v'",
				urlFlag,
				tokenFlag,
				allFlag,
				fullFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli fetch --url '%v' --token '%v' --all '%v' --full '%v' '%v'",
				urlFlag,
				tokenFlag,
				allFlag,
				fullFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Fetch(
				urlFlag,
				tokenFlag,
				allFlag,
				fullFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	urlDefVal := ""
	if v := ctx.Value(urlKey); v != nil {
		urlDefVal = ctx.Value(urlKey).(string)
	}

	cmd.Flags().StringP(
		"url",
		"u",
		urlDefVal,
		"FlockerHub URL (Example: https://flockerhub.com or http://flockerhub.com)")

	tokenDefVal := ""
	if v := ctx.Value(tokenKey); v != nil {
		tokenDefVal = ctx.Value(tokenKey).(string)
	}

	cmd.Flags().StringP(
		"token",
		"t",
		tokenDefVal,
		"Absolute path of the authentication token file")

	cmd.Flags().BoolP(
		"all",
		"a",
		false,
		"Fetch metadata of all volumesets that are available locally")

	cmd.Flags().BoolP(
		"full",
		"",
		false,
		"Report full UUIDs for objects instead of short UUIDs")

	return cmd
}

func newUpdateCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("update", []string{
			"[OPTIONS] VOLUMESET",
			"[OPTIONS] VOLUMESET:SNAPSHOT",
			"[OPTIONS] VOLUMESET:BRANCH",
			"[OPTIONS] VOLUMESET:VOLUME",
		}),
		Short: "Updates the object metadata",
		Long: `Updates the metadata of the object specified in the command. This command can be used to add and update the object's metadata.
The VOLUMESET, SNAPSHOT and VOLUME can be name or uuid. The BRANCH is always a name.
`,
		Example: `The following example explains how to update the volumeset or its objects

    $ fli update exampleVolSetName --description 'This is an updated descriptions' --attributes For=AfterUpdate --name newNameForExVolSetName

    $ fli update exampleVolSetName:exampleSnapshot --description 'This is an updated descriptions' --attributes For=AfterUpdate --name newNameForExSnapName

    $ fli update exampleVolSetName:exampleVol --attributes For=AfterUpdate --name newNameForExVolName
`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err             error
				nameFlag        string
				attributesFlag  string
				descriptionFlag string
				fullFlag        bool
			)

			nameFlag, err = cmd.Flags().GetString("name")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			attributesFlag, err = cmd.Flags().GetString("attributes")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			descriptionFlag, err = cmd.Flags().GetString("description")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			fullFlag, err = cmd.Flags().GetBool("full")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli update --name '%v' --attributes '%v' --description '%v' --full '%v' '%v'",
				nameFlag,
				attributesFlag,
				descriptionFlag,
				fullFlag,
				strings.Join(args, " "),
			)
			log.Printf("fli update --name '%v' --attributes '%v' --description '%v' --full '%v' '%v'",
				nameFlag,
				attributesFlag,
				descriptionFlag,
				fullFlag,
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Update(
				nameFlag,
				attributesFlag,
				descriptionFlag,
				fullFlag,
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	nameDefVal := ""
	if v := ctx.Value(nameKey); v != nil {
		nameDefVal = ctx.Value(nameKey).(string)
	}

	cmd.Flags().StringP(
		"name",
		"n",
		nameDefVal,
		"New name for the object")

	attributesDefVal := ""
	if v := ctx.Value(attributesKey); v != nil {
		attributesDefVal = ctx.Value(attributesKey).(string)
	}

	cmd.Flags().StringP(
		"attributes",
		"a",
		attributesDefVal,
		"A comma separated list of a key-value pairs(ex: userKey1=userVal1,userKey2=userVal2)")

	descriptionDefVal := ""
	if v := ctx.Value(descriptionKey); v != nil {
		descriptionDefVal = ctx.Value(descriptionKey).(string)
	}

	cmd.Flags().StringP(
		"description",
		"d",
		descriptionDefVal,
		"Update the description for VOLUMESET and SNAPSHOT only. This option is ignored for VOLUME and BRANCH")

	cmd.Flags().BoolP(
		"full",
		"",
		false,
		"Report full UUIDs for objects instead of short UUIDs")

	return cmd
}

func newVersionCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "version",
		Short: "Print the version information",
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
			)
			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli version '%v'",
				strings.Join(args, " "),
			)
			log.Printf("fli version '%v'",
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Version(
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	return cmd
}

func newInfoCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "info",
		Short: "Print the fli environment information",
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
			)
			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli info '%v'",
				strings.Join(args, " "),
			)
			log.Printf("fli info '%v'",
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Info(
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	return cmd
}

func newDiagnosticsCmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use: getMultiUseLine("diagnostics", []string{
			"DIRECTORY",
		}),
		Short: "Record the system state, and zip it for sending out to clusterhq for further inspection",
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
			)
			logger, err := newLogger()
			handleError(cmd, err)

			logger.Printf("fli diagnostics '%v'",
				strings.Join(args, " "),
			)
			log.Printf("fli diagnostics '%v'",
				strings.Join(args, " "),
			)

			var res Result
			res, err = h.Diagnostics(
				args,
			)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
				log.Printf("[ERROR] %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		},
	}

	return cmd
}

// CommandHandler inteface that implements handlers for cli commands
type CommandHandler interface {
	Clone(attributes string, full bool, args []string) (Result, error)
	Config(url string, token string, offline bool, args []string) (Result, error)
	Create(attributes string, full bool, args []string) (Result, error)
	Init(attributes string, description string, args []string) (Result, error)
	List(all bool, volume bool, snapshot bool, branch bool, full bool, args []string) (Result, error)
	Pull(url string, token string, full bool, args []string) (Result, error)
	Push(url string, token string, full bool, args []string) (Result, error)
	Remove(full bool, args []string) (Result, error)
	Setup(zpool string, force bool, args []string) (Result, error)
	Snapshot(branch string, newbranch bool, attributes string, description string, full bool, args []string) (Result, error)
	Sync(url string, token string, all bool, full bool, args []string) (Result, error)
	Fetch(url string, token string, all bool, full bool, args []string) (Result, error)
	Update(name string, attributes string, description string, full bool, args []string) (Result, error)
	Version(args []string) (Result, error)
	Info(args []string) (Result, error)
	Diagnostics(args []string) (Result, error)
}
