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

package cligen

import (
	"bytes"
	"fmt"
	"go/format"
	"io/ioutil"
	"log"
	"os"
	"text/template"
	"unicode"

	"github.com/go-yaml/yaml"
)

type (
	// commandFlags is part of the yaml structure that defines the flags for sub-commands
	commandFlags struct {
		Name  string
		Short string
		Type  string
		Value string
		Desc  string
	}

	// command is part of the yaml structure that defines a command and it's sub-commands
	command struct {
		Name        string
		Usage       []string
		Short       string
		Long        string
		SubCommands []*command
		Flags       []*commandFlags
		Handler     bool
		Completion  bool
		Prefix      string
		Aliases     []string
	}

	// Generator defines attributes need to generate go code from the yaml file
	Generator struct {
		buf     bytes.Buffer
		ctree   command
		pkg     string
		imports []string
	}

	// tmplHeader defines attributes that describes the header for generated go code
	tmplHeader struct {
		Pkg     string
		Imports []string
		Root    *command
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
 * CODE GENERATED AUTOMATICALLY WITH github.com/ClusterHQ/go/client/cmd/cmdgen
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */

package {{.Pkg}}

{{range .Imports}}import "{{.}}"
{{end}}


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
`

	// cmdTmpl template that generates cobra commands
	cmdTmpl = `func new{{firstCharToUpper .Prefix}}{{firstCharToUpper .Name}}Cmd(h handlerIface) *cobra.Command { {{if .Handler}}
	var jsonF = false
	{{else}}{{end}}
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
		{{end}} },{{else}}{{end}}{{if .Handler}}
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			{{range .Flags}}var {{.Name}}F {{.Type}}
			{{.Name}}F, err = cmd.Flags().Get{{firstCharToUpper .Type}}("{{.Name}}")
			if err != nil {
				return err
			}

			{{end}}var s []byte
			s, err = h.handler{{firstCharToUpper .Prefix}}{{firstCharToUpper .Name}}({{range .Flags}}
				{{.Name}}F,{{end}}
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
		}, {{else}}{{end}}
	}

	{{range .SubCommands}}cmd.AddCommand(new{{firstCharToUpper .Prefix}}{{firstCharToUpper .Name}}Cmd(h))
	{{end}}

	{{range .Flags}}cmd.Flags().{{firstCharToUpper .Type}}P("{{.Name}}", "{{.Short}}", {{if needsQuotes .Type}}"{{.Value}}"{{else}}{{.Value}}{{end}},"{{.Desc}}")
	{{end}}{{if .Completion}}
	var outputF = ""
	var complCmd = &cobra.Command {
		Use:   getMultiUseLine("completion", []string{"(--output <filepath> | -o <filepath>)"}),
		Short: "Generates bash completion file for dpcli command",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Parent().GenBashCompletionFile(outputF)
		},
	}

	complCmd.Flags().StringVarP(&outputF, "output", "o", "", "Generates bash completions file for dpcli")
	cmd.AddCommand(complCmd){{else}}{{end}}{{if .Handler}}

	cmd.Flags().BoolVarP(&jsonF, "json", "j", false, "Dump command output in JSON format")
	{{else}}{{end}}

	return cmd
}

`

	// ifaceTmpl template that generates interface handler
	ifaceTmpl = `{{if .Handler}}handler{{firstCharToUpper .Prefix}}{{firstCharToUpper .Name}}({{range .Flags}}{{.Name}} {{.Type}}, {{end}}args []string, jsonF bool) ([]byte, error)
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

func needsQuotes(t string) bool {
	if t == "string" {
		return true
	}

	return false
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
		"needsQuotes":      needsQuotes,
	}
	var t = template.Must(template.New("cmd").Funcs(fm).Parse(tmpl))
	var err = t.Execute(&g.buf, c)

	if err != nil {
		log.Fatalln(err)
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
	var buf = readAll(filepath)
	if err := yaml.Unmarshal(buf, &g.ctree); err != nil {
		log.Fatalf("yaml.Unmarshal failed with %s", err.Error())
		return
	}
}

// Generate the gocode for cobra commands read from yaml into Generator 'ctree'
func (g *Generator) Generate() {
	// imports for gocode
	var imports = []string{
		"strings",
		"encoding/json",
		"text/tabwriter",
		"bytes",
		"fmt",
		"os",
		"github.com/spf13/cobra",
	}
	var h = tmplHeader{
		Imports: imports,
		Pkg:     g.pkg,
		Root:    &g.ctree,
	}
	var fm = template.FuncMap{
		"firstCharToUpper": firstCharToUpper,
		"needsQuotes":      needsQuotes,
	}
	var tmpl = template.Must(template.New("cmd").Funcs(fm).Parse(hTmpl))
	var err = tmpl.Execute(&g.buf, h) // generate header
	if err != nil {
		log.Fatalln(err)
	}

	g.ProcessCommand("", &g.ctree, cmdTmpl) // generate cobra commands

	g.printf("type handlerIface interface {")
	g.ProcessCommand("", &g.ctree, ifaceTmpl) // generate interface for handlers for cli
	g.printf("}")
}

// Format the go code in Generator 'buf'
func (g *Generator) Format() []byte {
	src, err := format.Source(g.buf.Bytes())
	if err != nil {
		log.Printf("warning: internal error: invalid Go generated: %s", err)
		log.Printf("warning: compile the packages to analyze the error")
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
func readAll(file string) []byte {
	if !fileExists(file) {
		log.Printf("%s does not exist", file)
		return []byte{}
	}

	var fp *os.File
	var err error
	if fp, err = os.Open(file); err != nil {
		log.Fatalf("os.Open failed with %s", err.Error())
		return []byte{}
	}

	var buf []byte
	if buf, err = ioutil.ReadAll(fp); err != nil {
		log.Fatalf("ioutil.ReadAll failed with %s", err.Error())
		return []byte{}
	}

	return buf
}
