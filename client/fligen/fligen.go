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

package fligen

import (
	"bytes"
	"fmt"
	"go/format"
	"io/ioutil"
	"os"
	"strings"
	"text/template"
	"unicode"

	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/log"
	"github.com/go-yaml/yaml"
)

type (
	// commandFlags is part of the yaml structure that defines the flags for sub-commands
	commandFlags struct {
		// Flag name
		Name string
		// Short name
		Short string
		// Flag type
		Type string
		// Flag default value
		Value string
		// Description
		Desc string
	}

	// command is part of the yaml structure that defines a command and it's sub-commands
	command struct {
		// Command name
		Name string
		// Command usage
		Usage []string
		// Short description
		Short string
		// Long description
		Long string
		// Subcommands
		SubCommands []*command
		// Command flags
		Flags []*commandFlags
		// Requires handler
		Handler bool
		// Needs completion - only used for root
		Completion bool
		// Not used in YAML - command prefix for handler methods
		Prefix string
		// Alias for the command
		Aliases []string
		// Example
		Example string
	}

	// Generator defines attributes need to generate go code from the yaml file
	Generator struct {
		buf   bytes.Buffer
		ctree command
		pkg   string
	}

	// tmplHeader defines attributes that describes the header for generated go code
	tmplHeader struct {
		// Package name
		Pkg string
		// Import for the generated code
		Imports []string
		// Relative imports for the generated code
		RelImports []string
		// Root subcommand
		Root *command
	}
)

const (
	// hTmpl template that generates header
	hTmpl = `
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

package {{.Pkg}}

import (
{{range .Imports}}"{{.}}"
{{end}}
{{range .RelImports}}"{{.}}"
{{end}}
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
	dir := "/var/log/fli"
	logFile := filepath.Join(dir, "cmd.log")

	os.MkdirAll(dir, (os.ModeDir | 0755))

	fp, err := os.OpenFile(logFile, (os.O_CREATE | os.O_WRONLY | os.O_APPEND), 0666)
	if err != nil {
		return nil, err
	}

	return log.New(io.MultiWriter(fp), ""), nil
}

func init() {
	cobra.AddTemplateFunc("multiUseLine", multiUseLine)
}

type sortedCommands []*cobra.Command
func (c sortedCommands) Len() int           { return len(c) }
func (c sortedCommands) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c sortedCommands) Less(i, j int) bool { return c[i].Name() < c[j].Name() }

`

	// cmdTmpl template that generates cobra commands
	cmdTmpl = `
func new{{firstCharToUpper .Name}}Cmd(ctx context.Context, h CommandHandler) *cobra.Command {
	var cmd = &cobra.Command{ {{if len .Usage}}
		Use: getMultiUseLine("{{.Name}}", []string{
			{{range .Usage}}"{{.}}",
			{{end}}
		}),{{else}}
		Use: "{{.Name}}",{{end}}{{if len .Short}}
		Short: "{{.Short}}",{{else}}{{end}}{{if len .Long}}
		Long: ` + "`{{.Long}}`" + `,{{else}}{{end}}{{if len .Aliases}}
		Aliases: []string{ {{range .Aliases}}
			"{{.}}",
		{{end}} },{{else}}{{end}}{{if .Example}}
		Example: ` + "`{{.Example}}`" + `,{{else}}{{end}}{{if .Handler}}
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error{{range .Flags}}
				{{replaceDash .Name}}Flag {{.Type}}{{end}}
			){{range .Flags}}

			{{replaceDash .Name}}Flag, err = cmd.Flags().Get{{firstCharToUpper .Type}}("{{.Name}}")
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}
			{{end}}
			logger, err := newLogger()
			handleError(cmd, err)


			logger.Printf("{{.Prefix}} {{.Name}} {{range .Flags}}--{{.Name}} '%v' {{end}}'%v'",{{range .Flags}}
			{{replaceDash .Name}}Flag,{{end}}
			strings.Join(args, " "),
			)

			var res Result
			res, err = h.{{firstCharToUpper .Name}}({{range .Flags}}
				{{replaceDash .Name}}Flag,{{end}}
				args,
				)
			if err != nil {
				logger.Printf("ERROR: %v", err.Error())
			}

			handleError(cmd, err)
			displayOutput(cmd, res)
		}, {{else}}{{end}}
	}

	{{range .Flags}}{{if isStringType .Type}}{{if .Value}}
	{{.Name}}DefVal := "{{.Value}}"{{else}}
	{{.Name}}DefVal := ""
	if v := ctx.Value({{.Name}}Key); v != nil {
		{{.Name}}DefVal = ctx.Value({{.Name}}Key).(string)
	}
	{{end}}{{end}}
	cmd.Flags().{{firstCharToUpper .Type}}P(
		"{{.Name}}",
		"{{.Short}}",
		{{if isStringType .Type}}{{.Name}}DefVal{{else}}{{.Value}}{{end}},
		"{{.Desc}}")
	{{end}}

	{{if .Completion}}
	cmd.PersistentFlags().BoolP(
		"json",
		"",
		false,
		"Output using JSON format")

	var outputF = ""
	var complCmd = &cobra.Command {
		Use:   getMultiUseLine("completion", []string{"(--output <filepath> | -o <filepath>)"}),
		Short: "Generates bash completion file for fli command",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Parent().GenBashCompletionFile(outputF)
		},
	}
	complCmd.Flags().StringVarP(&outputF, "output", "o", "", "Generates bash completions file for fli"){{end}}

	{{if or .SubCommands .Completion}}
	sortedCmds := sortedCommands{
	{{range .SubCommands}}new{{firstCharToUpper .Name}}Cmd(ctx, h),
	{{end}}{{if .Completion}}complCmd,{{end}}
	}

	sort.Sort(sortedCmds)
	for _, c := range sortedCmds {
		cmd.AddCommand(c)
	}
	{{end}}

	return cmd
}
`

	// ifaceTmpl template that generates interface handler
	ifaceTmpl = `{{if .Handler}}{{firstCharToUpper .Name}}({{range .Flags}}{{replaceDash .Name}} {{.Type}}, {{end}}args []string) (Result, error)
{{end}}`
)

func firstCharToUpper(s string) string {
	if s == "" {
		return s
	}

	var a = []rune(s)
	a[0] = unicode.ToUpper(a[0])
	return string(a)
}

func isStringType(t string) bool {
	if t == "string" {
		return true
	}

	return false
}

func replaceDash(t string) string {
	return strings.Replace(t, "-", "", -1)
}

// ProcessCommand uses templates to generates the cobra command that is generated from the yaml file.
// This is a recursive method.
func (g *Generator) ProcessCommand(prefix string, c *command, tmpl string) {
	// update the prefix value too root parent name
	for _, sc := range c.SubCommands {
		sc.Prefix = c.Name
	}

	var fm = template.FuncMap{
		"firstCharToUpper": firstCharToUpper,
		"isStringType":     isStringType,
		"replaceDash":      replaceDash,
	}
	var t = template.Must(template.New("cmd").Funcs(fm).Parse(tmpl))
	var err = t.Execute(&g.buf, c)

	if err != nil {
		log.Fatalf("Failed to process command '%s': %s\n", c.Name, err.Error())
	}

	for _, sc := range c.SubCommands {
		g.ProcessCommand(c.Name, sc, tmpl)
	}
}

// printf write format specified string to Generator 'buf'
func (g *Generator) printf(format string, args ...interface{}) {
	fmt.Fprintf(&g.buf, format, args...)
}

// NewGenerator returns a cligen.Generator object used to translate yaml file into gocode for CLI
func NewGenerator(pkg string) *Generator {
	return &Generator{pkg: pkg}
}

// Unmarshal the yaml file to generate slice of command with flags into Generator 'ctree'
func (g *Generator) Unmarshal(filepath string) {
	buf, err := readAll(filepath)
	if err != nil {
		log.Fatalf("Failed to read: %s\n", err.Error())
		return
	}

	if err := yaml.Unmarshal(buf, &g.ctree); err != nil {
		log.Fatalf("yaml.Unmarshal failed with %s", err.Error())
		return
	}
}

// Generate the gocode for cobra commands read from yaml into Generator 'ctree'
func (g *Generator) Generate() {
	var h = tmplHeader{
		Imports: []string{
			"strings",
			"os",
			"path/filepath",
			"io",
			"sort",
			"fmt",
			"bytes",
			"text/tabwriter",
		},
		RelImports: []string{
			"github.com/spf13/cobra",
			"github.com/ClusterHQ/fli/log",
			"golang.org/x/net/context",
		},
		Pkg:  g.pkg,
		Root: &g.ctree,
	}

	var fm = template.FuncMap{
		"firstCharToUpper": firstCharToUpper,
		"isStringType":     isStringType,
	}
	var tmpl = template.Must(template.New("cmd").Funcs(fm).Parse(hTmpl))
	var err = tmpl.Execute(&g.buf, h) // generate header
	if err != nil {
		log.Fatalf("Failed to generate header: %s\n", err.Error())
	}

	g.ProcessCommand("", &g.ctree, cmdTmpl) // generate cobra commands

	g.printf("// CommandHandler inteface that implements handlers for cli commands\n")
	g.printf("type CommandHandler interface {")
	g.ProcessCommand("", &g.ctree, ifaceTmpl) // generate interface for handlers for cli
	g.printf("}")
}

// Format the go code in Generator 'buf'
func (g *Generator) Format() []byte {
	src, err := format.Source(g.buf.Bytes())
	if err != nil {
		log.Printf("warning: internal error: invalid Go generated: %s\n", err)
		log.Printf("warning: compile the packages to analyze the error\n")
		return g.buf.Bytes()
	}
	return src
}

// WriteOut the go code into a go file
func (g *Generator) WriteOut(file string) {
	if fileExists(file) {
		os.Remove(file)
	}

	var fp *os.File
	var err error
	if fp, err = os.Create(file); err != nil {
		log.Fatalf("os.Create failed with %s", err.Error())
		return
	}
	defer fp.Close()

	fp.Write([]byte(g.Format()))
}

// fileExists returns true if file exists
func fileExists(file string) bool {
	if _, err := os.Stat(file); err != nil {
		if !os.IsNotExist(err) {
			log.Fatalf("os.Stat for file %s failed with %s", file, err.Error())
		}
		return false
	}

	return true
}

// readAll the content of the file into []bytes and return
func readAll(file string) ([]byte, error) {
	if !fileExists(file) {
		return []byte{}, errors.Errorf("%s does not exist", file)
	}

	var fp *os.File
	var err error
	if fp, err = os.Open(file); err != nil {
		return []byte{}, errors.Errorf("Failed to open %s: %s\n", file, err.Error())
	}

	var buf []byte
	if buf, err = ioutil.ReadAll(fp); err != nil {
		return []byte{}, errors.Errorf("ioutil.ReadAll failed with %s", err.Error())
	}

	return buf, nil
}
