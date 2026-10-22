package space.gorogoro.kubotanutil2;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;

class MainProcess {
  public static final String DEBUG_TAG = "[DEBUG] ";
  public static final int LAST_EXECUTED_TYPE_BAD = 1;
  public static final int LAST_EXECUTED_TYPE_GOOD = 2;

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

  public static boolean status(Connection con, Player sp, Boolean isCommand) throws SQLException{
    PreparedStatement prepStmt;
    ResultSet rs;
    double reputation = 0;

    prepStmt = con.prepareStatement("SELECT reputation, last_executed_at FROM users WHERE uuid=?");
    prepStmt.setString(1,sp.getUniqueId().toString());
    rs = prepStmt.executeQuery();
    if(rs.next()){
      reputation = rs.getDouble(1);
      BukkitUtil.sendMessage(sp, " 現在の評価は " + String.format("%.3f", reputation) + " です");
      if(reputation < 1.000){
        BukkitUtil.sendMessage(sp, " reputationが1.000未満です！");
        BukkitUtil.sendMessage(sp, " goodしてもらえるようなアクションをしてみましょう！");
      }
    }

    if(isCommand) {
      BukkitUtil.sendMessage(sp, "=== 評価履歴を表示します ===");
      prepStmt = con.prepareStatement("SELECT reason,created_at,last_executed_type FROM last_executed_history WHERE target_uuid=? and created_at >= datetime('2021-01-01 00:00:00')");
      prepStmt.setString(1,sp.getUniqueId().toString());
      rs = prepStmt.executeQuery();
      String label = "";
      String created_at = "";
      int rownum = 0;
      SimpleDateFormat ymdhis = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      SimpleDateFormat ymd = new SimpleDateFormat("yyyy/MM/dd");
      Date datetime;
      while(rs.next()){
        rownum++;
        if(rs.getInt(3) == LAST_EXECUTED_TYPE_BAD){
          label="  bad ";
        }else if(rs.getInt(3) == LAST_EXECUTED_TYPE_GOOD){
          label="  good ";
        }
        created_at = "";
        if(!rs.getString(2).isEmpty()) {
          try {
            datetime = ymdhis.parse(rs.getString(2));
            created_at = ymd.format(datetime);
          } catch (Exception e) {
            BukkitUtil.sendMessage(sp, "評価の作成日取得でエラーが発生しました。");
          }
        }
        BukkitUtil.sendMessage(sp, label + "reason:" + rs.getString(1) + " (" + created_at + ")");
      }
      if(rownum <= 0) {
        BukkitUtil.sendMessage(sp, "  評価履歴は0件です");
      }
      BukkitUtil.sendMessage(sp, "※2021/01/01より古いデータは見れません");
    }
    return true;
  }

  public static boolean statusOp(Connection con, Player sp, Player online, String[] args) throws SQLException{
    if(args.length != 1){
      return false;
    }

    PreparedStatement prepStmt;
    ResultSet rs;
    // ターゲットプレイヤーを取得する
    OfflinePlayer tp = BukkitUtil.getPlayer(con, online, args[0]);
    if(tp == null){
      BukkitUtil.sendMessage(sp, "["+args[0]+"] not found.");
      BukkitUtil.sendMessage(sp, "ターゲット未確認");
      return true;
    }
    prepStmt = con.prepareStatement("SELECT reputation, last_executed_at FROM users WHERE uuid=?");
    prepStmt.setString(1,tp.getUniqueId().toString());
    rs = prepStmt.executeQuery();
    if(rs.next()){
      BukkitUtil.sendMessage(sp, 
        " reputation:" + String.format("%.3f", rs.getDouble(1)) +
        " last_executed_at:" + rs.getString(2)
      );
    }

    BukkitUtil.sendMessage(sp, "=== 評価履歴を表示します ===");
    prepStmt = con.prepareStatement("SELECT reason,sender_playername,created_at,last_executed_type FROM last_executed_history WHERE target_uuid=? and created_at >= datetime('2021-01-01 00:00:00')");
    prepStmt.setString(1,tp.getUniqueId().toString());
    rs = prepStmt.executeQuery();
    String label = "";
    String created_at = "";
    int rownum = 0;
    SimpleDateFormat ymdhis = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat ymd = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Date datetime;
    while(rs.next()){
      rownum++;
      if(rs.getInt(4) == LAST_EXECUTED_TYPE_BAD){
        label="  bad ";
      }else if(rs.getInt(4) == LAST_EXECUTED_TYPE_GOOD){
        label="  good ";
      }
      created_at = "";
      if(!rs.getString(3).isEmpty()) {
        try {
          datetime = ymdhis.parse(rs.getString(3));
          created_at = ymd.format(datetime);
        } catch (Exception e) {
          BukkitUtil.sendMessage(sp, "評価の作成日取得でエラーが発生しました。");
        }
      }
      BukkitUtil.sendMessage(sp, label + "reason:" + rs.getString(1) + " by " + rs.getString(2) + " (" + created_at + ")");
    }
    if(rownum <= 0) {
      BukkitUtil.sendMessage(sp, "  評価履歴は0件です");
    }
    BukkitUtil.sendMessage(sp, "※2021/01/01より古いデータは見れません");

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

  public static boolean reasonOp(Connection con, Player sp, Player online, String[] args) throws SQLException{
    if(args.length != 1){
      return false;
    }

    PreparedStatement prepStmt;
    ResultSet rs;
    // ターゲットプレイヤーを取得する
    OfflinePlayer tp = BukkitUtil.getPlayer(con, online, args[0]);
    if(tp == null){
      BukkitUtil.sendMessage(sp, "["+args[0]+"] not found.");
      BukkitUtil.sendMessage(sp, "ターゲット未確認");
      return true;
    }
    prepStmt = con.prepareStatement("SELECT reason,sender_playername,created_at,last_executed_type FROM last_executed_history WHERE target_uuid=?");
    prepStmt.setString(1,tp.getUniqueId().toString());
    rs = prepStmt.executeQuery();
    String label = "";
    while(rs.next()){
      if(rs.getInt(4) == LAST_EXECUTED_TYPE_BAD){
        label="bad ";
      }else if(rs.getInt(4) == LAST_EXECUTED_TYPE_GOOD){
        label="good ";
      }
      BukkitUtil.sendMessage(sp, label + "reason:" + rs.getString(1) + " by " + rs.getString(2) + " (" + rs.getString(3) + ")");
    }
    return true;
  }

  public static boolean bad(FileConfiguration conf, Connection con, Player sp, Player online, String[] args){
    return sub(LAST_EXECUTED_TYPE_BAD, conf, con, sp, online, args);
  }

  public static boolean good(FileConfiguration conf,Connection con, Player sp, Player online, String[] args) throws SQLException{
    return sub(LAST_EXECUTED_TYPE_GOOD, conf, con, sp, online, args);
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

  public static boolean sub(int type, FileConfiguration conf, Connection con, Player sp, Player online, String[] args){
    try{
      PreparedStatement prepStmt;
      ResultSet rs;

      if(args.length != 2){
        return false;
      }

      if(args[1].length() < 1){
        BukkitUtil.sendMessage(sp, "理由を入力してください。（スペースは文中に含めないでください。）");
        BukkitUtil.sendMessage(sp, "例：/bad プレイヤー名 理由");
        return false;
      }
      String reason = args[1];

      int execIntervalHour = conf.getInt("exex_interval_hour");
      boolean isDebug = conf.getBoolean("is_debug");

      // 送信者のexecuted_atを取得する
      long lastExecutedAt=0;
      prepStmt = con.prepareStatement("SELECT last_executed_at FROM users WHERE uuid=?");
      prepStmt.setString(1,sp.getUniqueId().toString());
      rs = prepStmt.executeQuery();
      if(rs.next()){
        lastExecutedAt = DateTimeUtil.getTimestampByStrings(rs.getString(1));
        if(isDebug){
          BukkitUtil.sendMessage(sp, DEBUG_TAG + "送信者情報確認完了");
        }
      }

      // ターゲットプレイヤーを取得する
      OfflinePlayer tp = BukkitUtil.getPlayer(con, online, args[0]);
      if(tp == null){
        BukkitUtil.sendMessage(sp, "["+args[0]+"] not found.");
        BukkitUtil.sendMessage(sp, "指定されたプレイヤーが見つかりません");
        return true;
      }

      if(tp.getUniqueId().equals(sp.getUniqueId())){
        BukkitUtil.sendMessage(sp, "自分への評価はできません");
        return true;
      }

      // ターゲットのreputationを取得する
      double reputation=0;
      prepStmt = con.prepareStatement("SELECT reputation FROM users WHERE uuid=?");
      prepStmt.setString(1,tp.getUniqueId().toString());
      rs = prepStmt.executeQuery();
      if(rs.next()){
        reputation = rs.getDouble(1);
        if(isDebug){
          BukkitUtil.sendMessage(sp, DEBUG_TAG + "ターゲット情報確認完了");
        }
      }
      boolean isJailed = false;
      if(reputation < 0){
        isJailed = true;
        if(isDebug){
          BukkitUtil.sendMessage(sp, DEBUG_TAG + "評価値はマイナスです");
        }
      }else if(reputation >= 1.000 && type == LAST_EXECUTED_TYPE_GOOD && !sp.isOp()){
        BukkitUtil.sendMessage(sp, "評価値上限を超えて評価することはできません");
        return true;
      }

      // 送信者のlast_executed_atを参照しexecIntervalHour時間以内であれば処理をスキップする
      long limitTs = DateTimeUtil.addHour((execIntervalHour * -1)).getTimeInMillis();
      if(isDebug){
        BukkitUtil.sendMessage(sp, DEBUG_TAG + DateTimeUtil.getStringByTimestamp(lastExecutedAt) + " < " + DateTimeUtil.getStringByTimestamp(limitTs));
      }
      if(lastExecutedAt < limitTs ){

        if(isDebug){
          BukkitUtil.sendMessage(sp, DEBUG_TAG + "ターゲット確認");
        }
        // ヒストリーの登録済みチェック
        Boolean existsHistory = false;
        prepStmt = con.prepareStatement("SELECT COUNT(id) FROM last_executed_history "
            + " WHERE sender_uuid=? AND target_uuid=? AND last_executed_type=?");
        prepStmt.setString(1,sp.getUniqueId().toString());
        prepStmt.setString(2,tp.getUniqueId().toString());
        prepStmt.setInt(3,type);
        rs = prepStmt.executeQuery();
        while(rs.next()){
          if(rs.getString(1).equals("1")){
            existsHistory = true;
          }
        }
        String typeMessage = "";
        if(type == LAST_EXECUTED_TYPE_BAD){
          typeMessage = "減算";
        }else if(type == LAST_EXECUTED_TYPE_GOOD){
          typeMessage = "加算";
        }
        if(existsHistory){
          BukkitUtil.sendMessage(sp, "同一ユーザーへの" + typeMessage + "の再評価は許可されていません");
          return true;
        }

        // 送信者のlast_executed_atを更新する
        prepStmt = con.prepareStatement("UPDATE users SET last_executed_at=? WHERE uuid=?");
        prepStmt.setString(1, DateTimeUtil.getNowDateTime());
        prepStmt.setString(2, sp.getUniqueId().toString());
        prepStmt.addBatch();
        prepStmt.executeBatch();

        if(isDebug){
          BukkitUtil.sendMessage(sp, DEBUG_TAG + "メイン処理実行開始");
        }
        String updateSql = "";
        double updateValue = 0;
        switch(type){
        case LAST_EXECUTED_TYPE_BAD:
          updateSql = "UPDATE users SET reputation=reputation - ?, last_executed_at=?, updated_at=? WHERE uuid = ?";
          updateValue = conf.getDouble("bad_value");
          break;

        case LAST_EXECUTED_TYPE_GOOD:
          updateSql = "UPDATE users SET reputation=reputation + ?, last_executed_at=?, updated_at=? WHERE uuid = ?";
          updateValue = conf.getDouble("good_value");
          break;

        default:
          break;
        }

        prepStmt= con.prepareStatement(updateSql);
        prepStmt.setDouble(1, updateValue);
        prepStmt.setString(2, DateTimeUtil.getNowDateTime());
        prepStmt.setString(3, DateTimeUtil.getNowDateTime());
        prepStmt.setString(4, tp.getUniqueId().toString());
        prepStmt.addBatch();
        prepStmt.executeBatch();
        BukkitUtil.sendMessage(sp, "指定されたプレイヤーの評価値を" + typeMessage + "しました");

        // ヒストリーが無ければ登録
        if(!existsHistory){
          prepStmt = con.prepareStatement(
            "INSERT INTO last_executed_history("
            + " sender_uuid"
            + ",sender_playername"
            + ",target_uuid"
            + ",target_playername"
            + ",last_executed_type"
            + ",reason"
            + ") VALUES ("
            + " ?"
            + ",?"
            + ",?"
            + ",?"
            + ",?"
            + ",?"
            + ");"
          );
          prepStmt.setString(1, sp.getUniqueId().toString());
          prepStmt.setString(2, sp.getName());
          prepStmt.setString(3, tp.getUniqueId().toString());
          prepStmt.setString(4, targetName(tp, args[0]));
          prepStmt.setInt(5, type);
          prepStmt.setString(6, reason);
          prepStmt.addBatch();
          prepStmt.executeBatch();
        }
        con.commit();

        reputation=0;
        prepStmt = con.prepareStatement("SELECT reputation FROM users WHERE uuid=?");
        prepStmt.setString(1,tp.getUniqueId().toString());
        rs = prepStmt.executeQuery();
        if(rs.next()){
          reputation = rs.getDouble(1);
          if(isDebug){
            BukkitUtil.sendMessage(sp, DEBUG_TAG + "ターゲット情報確認完了");
          }
        }

        if(reputation < 0){
          if(isJailed == false){
            jail_proc(con, conf, sp, tp, targetName(tp, args[0]));
          }else{
            BukkitUtil.sendMessage(sp, "投獄済みです");
          }
        }else{
          BukkitUtil.sendMessage(sp, "評価が完了しました");
        }
      }else{
        BukkitUtil.sendMessage(sp, "初ログインもしくは最終実行から" + execIntervalHour + "時間以内は実行できません");
      }
    } catch (SQLException e) {
      BukkitUtil.logStackMessage(e);
      try {
        con.rollback();
      } catch (SQLException e1) {
        BukkitUtil.logStackMessage(e1);
      } catch (Exception e2) {
        BukkitUtil.logStackMessage(e2);
      }
    }
    return true;
  }

  public static boolean upDate(Connection con, Player sp, String[] args) throws SQLException{
    PreparedStatement prepStmt;

    if(args.length != 2){
      return false;
    }

    prepStmt = con.prepareStatement("UPDATE users SET last_executed_at=? WHERE uuid=?");
    prepStmt.setString(1, args[0] + " " + args[1]);
    prepStmt.setString(2, sp.getUniqueId().toString());
    prepStmt.addBatch();
    prepStmt.executeBatch();
    con.commit();
    BukkitUtil.sendMessage(sp, "更新しました");
    return true;
  }

  public static boolean upValue(Connection con, Player sp, Player online, String[] args) throws SQLException{
    PreparedStatement prepStmt;

    if(args.length != 2){
      return false;
    }

    // ターゲットプレイヤーを取得する
    OfflinePlayer tp = BukkitUtil.getPlayer(con, online, args[0]);
    if(tp == null){
      BukkitUtil.sendMessage(sp, "["+args[0]+"] not found.");
      BukkitUtil.sendMessage(sp, "ターゲット未確認");

      return true;
    }

    prepStmt = con.prepareStatement("UPDATE users SET reputation=? WHERE uuid=?");
    prepStmt.setString(1, args[1]);
    prepStmt.setString(2, tp.getUniqueId().toString());
    prepStmt.addBatch();
    prepStmt.executeBatch();
    con.commit();
    BukkitUtil.sendMessage(sp, "更新しました");
    return true;
  }
}
