package space.gorogoro.kubotanutil2;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.bukkit.OfflinePlayer;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;

class MainProcess {
  public static final int IS_JAILED_TRUE = 1;
  public static final int IS_JAILED_FALSE = 2;
  
  public static final int IS_BANED_TRUE = 1;
  public static final int IS_BANED_FALSE = 2;

  // オフラインのプレイヤーは getName() が null のことがあるので、そのときはコマンドの引数の名前を使う
  private static String targetName(OfflinePlayer tp, String argName){
    String name = tp.getName();
    return name != null ? name : argName;
  }

  public static boolean ban(KubotanUtil2 plugin,Connection con , FileConfiguration conf, Player sp, String[] args) throws SQLException{
      if(args.length != 2){
        return false;
      }
      BukkitUtil.dispatchCommand(String.format(conf.getString("ban_command"), args[0], args[1]));

      PreparedStatement prepStmt = con.prepareStatement("UPDATE users SET is_baned=? WHERE playername=? COLLATE NOCASE");
      prepStmt.setInt(1, IS_BANED_TRUE);
      prepStmt.setString(2, args[0]);
      prepStmt.addBatch();
      prepStmt.executeBatch();
      con.commit();
      BukkitUtil.sendMessage(sp, args[0] + "さんをBANしました。");
      return true;
  }
  
  public static boolean unban(KubotanUtil2 plugin,Connection con , FileConfiguration conf, Player sp, String[] args) throws SQLException{
    if(args.length != 1){
      return false;
    }
    BukkitUtil.dispatchCommand(String.format(conf.getString("unban_command"), args[0]));

    PreparedStatement prepStmt = con.prepareStatement("UPDATE users SET is_baned=? WHERE playername=? COLLATE NOCASE");
    prepStmt.setInt(1, IS_BANED_FALSE);
    prepStmt.setString(2, args[0]);
    prepStmt.addBatch();
    prepStmt.executeBatch();
    con.commit();
    BukkitUtil.sendMessage(sp, args[0] + "さんをUNBANしました。");
    return true;
  }

  public static boolean isJailed(Connection con, Player sp, String[] args) throws SQLException{
    if(args.length != 1){
      return false;
    }
    BukkitUtil.sendMessage(sp, args[0] + "さんの投獄・BAN状況：");
    PreparedStatement prepStmt;
    ResultSet rs;
    Boolean isJailed = false;
    Boolean isBaned = false;
    prepStmt = con.prepareStatement("SELECT is_jailed,is_baned FROM users WHERE playername=? COLLATE NOCASE");
    prepStmt.setString(1, args[0]);
    rs = prepStmt.executeQuery();
    if(rs.next()){
      if(rs.getInt(1) == IS_JAILED_TRUE) {
        isJailed = true;
      }
      if(rs.getInt(2) == IS_BANED_TRUE) {
        isBaned = true;
      }
    }
    if(isJailed) {
      BukkitUtil.sendMessage(sp, " ・投獄中です");
    }else {
      BukkitUtil.sendMessage(sp, " ・投獄中ではありません");
    }
    if(isBaned) {
      BukkitUtil.sendMessage(sp, " ・BANされています");
    }else {
      BukkitUtil.sendMessage(sp, " ・BANされていません");
    }
    return true;
  }

  public static boolean jail(KubotanUtil2 plugin, Connection con , FileConfiguration conf, Player sp, Player online, String[] args){
    try{
      if(args.length != 1){
        return false;
      }

      OfflinePlayer tp = BukkitUtil.getPlayer(con, online, args[0]);
      if(tp == null){
        BukkitUtil.sendMessage(sp, "["+args[0]+"] not found.");
        BukkitUtil.sendMessage(sp, "指定されたプレイヤーが見つかりません");
        return true;
      }

      jail_proc(con, conf, sp, tp, targetName(tp, args[0]));

    } catch (Exception e) {
      BukkitUtil.logStackMessage(e);
    }
    return true;
  }

  public static boolean jail_proc(Connection con , FileConfiguration conf, Player sp, OfflinePlayer tp, String targetName) throws SQLException {

    if(tp.getUniqueId().equals(sp.getUniqueId())){
      BukkitUtil.sendMessage(sp, "自分の投獄はできません");
      return true;
    }

    String commandString = "";
    commandString = String.format(conf.getString("jail_command"), targetName);
    BukkitUtil.dispatchCommand(commandString);
    commandString = String.format(conf.getString("mute_command"), targetName);
    BukkitUtil.dispatchCommand(commandString);

    PreparedStatement prepStmt = con.prepareStatement("UPDATE users SET is_jailed=? WHERE uuid=?");
    prepStmt.setInt(1, 1);
    prepStmt.setString(2, tp.getUniqueId().toString());
    prepStmt.addBatch();
    prepStmt.executeBatch();
    con.commit();
    BukkitUtil.sendMessage(sp, "投獄しました。");
    return true;
  }

  public static boolean unjail(KubotanUtil2 plugin, Connection con, FileConfiguration conf, Player sp, Player online, String[] args){
    try{
      if(args.length != 1){
        return false;
      }

      OfflinePlayer tp = BukkitUtil.getPlayer(con, online, args[0]);
      if(tp == null){
        BukkitUtil.sendMessage(sp, "["+args[0]+"] not found.");
        BukkitUtil.sendMessage(sp, "指定されたプレイヤーが見つかりません");
        return true;
      }

      if(tp.getUniqueId().equals(sp.getUniqueId())){
        BukkitUtil.sendMessage(sp, "自分の釈放はできません");
        return true;
      }

      String commandString = "";
      commandString = String.format(conf.getString("unjail_command"), targetName(tp, args[0]));
      BukkitUtil.dispatchCommand(commandString);
      commandString = String.format(conf.getString("unmute_command"), targetName(tp, args[0]));
      BukkitUtil.dispatchCommand(commandString);
      commandString = String.format(conf.getString("tp_command"), targetName(tp, args[0]), sp.getName());
      BukkitUtil.dispatchCommand(commandString);

      PreparedStatement prepStmt = con.prepareStatement("UPDATE users SET is_jailed=? WHERE uuid=?");
      prepStmt.setInt(1, 0);
      prepStmt.setString(2, tp.getUniqueId().toString());
      prepStmt.addBatch();
      prepStmt.executeBatch();
      con.commit();

      BukkitUtil.sendMessage(sp, "釈放処理(unjail及びmute)を実行しました。");

    } catch (Exception e) {
      BukkitUtil.logStackMessage(e);
    }
    return true;
  }
}
