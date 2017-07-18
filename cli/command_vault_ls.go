package cli

import (
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	vaultListCommand = vaultCommands.Command("list", "List contents of a vault").Alias("ls").Hidden()
	vaultListPrefix  = vaultListCommand.Flag("prefix", "Prefix").String()
)

func init() {
	vaultListCommand.Action(listVaultContents)
}

func listVaultContents(context *kingpin.ParseContext) error {
	rep := mustConnectToRepository(nil)

	entries, err := rep.Vault.List(*vaultListPrefix, -1)
	if err != nil {
		return err
	}

	for _, e := range entries {
		fmt.Println(e)
	}

	return nil
}
