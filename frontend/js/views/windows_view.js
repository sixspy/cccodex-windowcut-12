/** 业务窗页签模板片段 */
export const windowsViewTemplate = `
<div class="card wide" v-show="tab===0">
  <h2>窗口条带</h2>
  <p class="muted">关窗与物化封口合一：同库事务一次提交，占合一锁 seal_unify，后到只记争用；期间入账冻结，无可脏间隙。</p>
  <table>
    <thead><tr><th>窗口</th><th>世代</th><th>开启</th><th>关闭</th><th>状态</th><th>操作</th></tr></thead>
    <tbody>
      <tr v-for="w in windows" :key="w.id">
        <td>{{w.name}}</td>
        <td>{{w.generation}}</td>
        <td>{{w.open_at}}</td>
        <td>{{w.close_at}}</td>
        <td>{{w.status}}</td>
        <td>
          <button class="ghost" v-if="w.status!=='已关闭'" @click="sealWin(w.id)">关窗物化（封口合一）</button>
          <button class="ghost" @click="loadRecon(w.id)">对账</button>
          <button class="ghost" @click="reopenStub(w.id)">重开空桩</button>
        </td>
      </tr>
    </tbody>
  </table>
  <div class="err">{{actionErr}}</div>
</div>
`;
