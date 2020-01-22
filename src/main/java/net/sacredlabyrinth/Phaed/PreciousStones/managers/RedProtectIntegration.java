package net.sacredlabyrinth.Phaed.PreciousStones.managers;

import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.entity.Player;
import org.bukkit.plugin.Plugin;

import net.sacredlabyrinth.Phaed.PreciousStones.PreciousStones;
import net.sacredlabyrinth.Phaed.PreciousStones.field.FieldSettings;

import br.net.fabiozumbi12.RedProtect.Bukkit.API.RedProtectAPI;
import br.net.fabiozumbi12.RedProtect.Bukkit.RedProtect;
import br.net.fabiozumbi12.RedProtect.Bukkit.Region;

public class RedProtectIntegration {
    private PreciousStones plugin;
    private RedProtectAPI redProtectAPI;

    /**
     *
     */
    public RedProtectIntegration() {
        plugin = PreciousStones.getInstance();
        this.redProtectAPI = RedProtect.get().getAPI();
    }

    private boolean checkRedProtect() {
        Plugin pRP = Bukkit.getPluginManager().getPlugin("RedProtect");
        return pRP != null && pRP.isEnabled();
    }

    public boolean isRegion(Block block) {
        try {
            Region region = redProtectAPI.getRegion(block.getLocation());

            return region != null;
        } catch (Exception ex) {
            return false;
        }
    }

    public boolean canBuild(Player player, Location loc) {
        try {
            // if null passed then pick up some random player

            if (player == null) {
                player = plugin.getServer().getWorlds().get(0).getPlayers().get(0);
            }

            if (player == null) {
                return false;
            }

            Region region = redProtectAPI.getRegion(loc);

            if (region == null) {
                return true;
            }

            return region.canBuild(player);
        } catch (Exception ex) {
            return true;
        }
    }

    public boolean canBuildField(Player player, Block block, FieldSettings fs) {
        Location loc = block.getLocation();

        World w = loc.getWorld();
        int x = loc.getBlockX();
        int y = loc.getBlockY();
        int z = loc.getBlockZ();
        int radius = fs.getRadius();

        if (canBuild(player, new Location(w, x + radius, y + radius, z + radius))) {
            if (canBuild(player, new Location(w, x + radius, y + radius, z - radius))) {
                if (canBuild(player, new Location(w, x + radius, y - radius, z + radius))) {
                    if (canBuild(player, new Location(w, x + radius, y - radius, z - radius))) {
                        if (canBuild(player, new Location(w, x - radius, y + radius, z + radius))) {
                            if (canBuild(player, new Location(w, x - radius, y + radius, z - radius))) {
                                if (canBuild(player, new Location(w, x - radius, y - radius, z + radius))) {
                                    if (canBuild(player, new Location(w, x - radius, y - radius, z - radius))) {
                                        if (canBuild(player, new Location(w, x, y, z))) {
                                            return true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return false;
    }
}
