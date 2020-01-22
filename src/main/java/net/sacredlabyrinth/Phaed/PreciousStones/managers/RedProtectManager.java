package net.sacredlabyrinth.Phaed.PreciousStones.managers;

import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.block.Block;
import org.bukkit.entity.Player;
import org.bukkit.plugin.Plugin;

import net.sacredlabyrinth.Phaed.PreciousStones.field.FieldSettings;

public class RedProtectManager {
    private RedProtectIntegration rp;

    /**
     *
     */
    public RedProtectManager() {
        Plugin pRP = Bukkit.getPluginManager().getPlugin("RedProtect");
        if (pRP != null && pRP.isEnabled()) {
            rp = new RedProtectIntegration();
        }
    }

    public boolean isRegion(Block block) {
        return rp == null ? false : rp.isRegion(block);
    }

    public boolean canBuild(Player player, Location loc) {
        return rp == null ? true : rp.canBuild(player, loc);
    }

    public boolean canBuildField(Player player, Block block, FieldSettings fs) {
        return rp == null ? true : rp.canBuildField(player, block, fs);
    }
}
