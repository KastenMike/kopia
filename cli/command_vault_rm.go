package cli

import "gopkg.in/alecthomas/kingpin.v2"

var (
	vaultRemoveCommand = vaultCommands.Command("rm", "Remove vault items").Hidden()
	vaultRemoveItems   = vaultRemoveCommand.Arg("item", "Items to remove").Strings()
)

func init() {
	vaultRemoveCommand.Action(removeVaultItem)
}

func removeVaultItem(context *kingpin.ParseContext) error {
	rep := mustConnectToRepository(nil)

	for _, v := range *vaultRemoveItems {
		if err := rep.Vault.Remove(v); err != nil {
			return err
		}
	}

	return nil
}
