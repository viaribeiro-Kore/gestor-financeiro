# app.py (VERSÃO FINAL COM CORREÇÃO DE ASYNCIO)
import streamlit as st
from libsql_client import create_client, Statement
import pandas as pd
from datetime import datetime
import io
import asyncio

# --- FUNÇÃO DE CONEXÃO AO BANCO TURSO ---
def get_turso_client():
    """Cria e retorna um cliente de conexão com o Turso, usando os Secrets do Streamlit."""
    url = st.secrets["turso"]["db_url"]
    token = st.secrets["turso"]["auth_token"]
    return create_client(url=url, auth_token=token)

# --- FUNÇÕES DE DADOS AGORA SÃO ASSÍNCRONAS ---
async def get_all_data():
    """Busca todos os dados necessários do banco em uma única conexão."""
    client = get_turso_client()
    try:
        results = await client.batch([
            "SELECT t.*, c.name as contact_name FROM transactions t LEFT JOIN contacts c ON t.contact_id = c.id ORDER BY t.id DESC",
            """
            SELECT
                r.id as refund_id, r.status, r.refund_to_contact_id,
                c.name as refund_to_name, t.id as transaction_id, t.description,
                t.amount, t.payment_date
            FROM refunds r
            JOIN transactions t ON r.transaction_id = t.id
            LEFT JOIN contacts c ON r.refund_to_contact_id = c.id
            ORDER BY r.status, r.id DESC
            """,
            "SELECT * FROM contacts ORDER BY name"
        ])
        transactions_df = pd.DataFrame(results[0].rows, columns=results[0].columns)
        refunds_df = pd.DataFrame(results[1].rows, columns=results[1].columns)
        contacts_df = pd.DataFrame(results[2].rows, columns=results[2].columns)
    finally:
        await client.close()
    return transactions_df, refunds_df, contacts_df

async def delete_transaction(transaction_id):
    client = get_turso_client()
    try:
        await client.batch([
            Statement("DELETE FROM refunds WHERE transaction_id = ?", [transaction_id]),
            Statement("DELETE FROM transactions WHERE id = ?", [transaction_id])
        ])
    finally:
        await client.close()
    st.success("Lançamento e/ou reembolso associado deletado com sucesso!")

async def add_new_transaction(description, amount, trans_type, status, payment_date, category, contact_id, is_refund):
    client = get_turso_client()
    try:
        rs = await client.execute(
            "INSERT INTO transactions (description, amount, type, status, payment_date, category, contact_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (description, amount, trans_type, status, str(payment_date), category, contact_id)
        )
        if is_refund:
            transaction_id = rs.last_insert_rowid
            await client.execute("INSERT INTO refunds (transaction_id, status) VALUES (?, ?)", (transaction_id, "Pendente"))
            st.sidebar.success("Pagamento registrado e marcado para reembolso!")
        else:
            st.sidebar.success("Lançamento adicionado!")
    finally:
        await client.close()

async def add_new_contact(name, doc, ctype, notes):
    client = get_turso_client()
    try:
        await client.execute("INSERT INTO contacts (name, document, type, notes) VALUES (?, ?, ?, ?)", (name, doc, ctype, notes))
        st.success(f"Contato '{name}' adicionado com sucesso!")
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            st.error(f"Erro: O contato '{name}' já existe.")
        else:
            st.error(f"Ocorreu um erro: {e}")
    finally:
        await client.close()

async def update_refund_contact(contact_id, refund_id):
    client = get_turso_client()
    try:
        await client.execute("UPDATE refunds SET refund_to_contact_id = ? WHERE id = ?", (contact_id, refund_id))
    finally:
        await client.close()

async def mark_refund_as_done(refund_id):
    client = get_turso_client()
    try:
        await client.execute("UPDATE refunds SET status = 'Concluído' WHERE id = ?", (refund_id,))
    finally:
        await client.close()


def convert_dfs_to_excel(transactions_df, refunds_df, contacts_df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        trans_export = transactions_df.rename(columns={'id': 'ID Transação', 'description': 'Descrição', 'amount': 'Valor','type': 'Tipo', 'status': 'Status', 'payment_date': 'Data Pagamento','category': 'Categoria', 'contact_name': 'Contato'})
        trans_export[['ID Transação', 'Descrição', 'Valor', 'Tipo', 'Status', 'Data Pagamento', 'Categoria', 'Contato']].to_excel(writer, index=False, sheet_name='Transações')
        refund_export = refunds_df.rename(columns={'transaction_id': 'ID Transação Original', 'description': 'Descrição', 'amount': 'Valor','refund_to_name': 'Reembolsar Para', 'status': 'Status Reembolso'})
        refund_export[['ID Transação Original', 'Descrição', 'Valor', 'Reembolsar Para', 'Status Reembolso']].to_excel(writer, index=False, sheet_name='Reembolsos')
        contacts_df.to_excel(writer, index=False, sheet_name='Contatos')
    return output.getvalue()


# --- APP PRINCIPAL ---
def main():
    st.set_page_config(page_title="Meu Gestor Financeiro", layout="wide")
    st.title("📊 Gestor Financeiro Pro (Cloud)")

    if 'transaction_to_delete' not in st.session_state:
        st.session_state.transaction_to_delete = None

    try:
        transactions_df, refunds_df, contacts_df = asyncio.run(get_all_data())
    except Exception as e:
        st.error(f"Não foi possível conectar ao banco de dados: {e}")
        st.stop()

    contact_names = ["-- Nenhum --"] + contacts_df['name'].tolist()

    if not transactions_df.empty:
        transactions_df['amount'] = pd.to_numeric(transactions_df['amount'])

    # --- SIDEBAR ---
    st.sidebar.header("Novo Lançamento")
    with st.sidebar.form(key="transaction_form", clear_on_submit=True):
        description = st.text_input("Descrição*")
        amount = st.number_input("Valor (US$)*", min_value=0.01, format="%.2f")
        trans_type = st.selectbox("Tipo", ["❌ Pagamento", "✅ Recebimento"])
        selected_contact_name = st.selectbox("Contato", options=contact_names)
        category = st.text_input("Categoria")
        status = st.selectbox("Status", ["Pago/Recebido", "A Pagar/Receber"])
        is_refund = False
        if trans_type == "❌ Pagamento":
            is_refund = st.checkbox("Marcar para Reembolso?")
        payment_date = st.date_input("Data de Pagamento/Recebimento")
        submit_button = st.form_submit_button(label="Adicionar Lançamento")

        if submit_button:
            if not description or amount <= 0:
                st.sidebar.error("Descrição e Valor são obrigatórios.")
            else:
                contact_id = None
                if selected_contact_name != "-- Nenhum --":
                    contact_id = int(contacts_df[contacts_df['name'] == selected_contact_name]['id'].iloc[0])
                asyncio.run(add_new_transaction(description, amount, trans_type, status, payment_date, category, contact_id, is_refund))
                st.rerun()

    # --- ABAS ---
    tab1, tab2, tab3, tab4 = st.tabs(["Dashboard", "Todos os Lançamentos", "Reembolsos 🧾", "Contatos 👤"])

    with tab1:
        st.header("Resumo Financeiro (Caixa Realizado)")
        df_realizado = transactions_df[transactions_df['status'] == 'Pago/Recebido'].copy()
        if not df_realizado.empty:
            total_recebido = df_realizado[df_realizado['type'] == '✅ Recebimento']['amount'].sum()
            total_pago = df_realizado[df_realizado['type'] == '❌ Pagamento']['amount'].sum()
            saldo = total_recebido - total_pago
            col1, col2, col3 = st.columns(3); col1.metric("Total Recebido", f"US$ {total_recebido:,.2f}"); col2.metric("Total Pago", f"US$ {total_pago:,.2f}"); col3.metric("Saldo em Caixa", f"US$ {saldo:,.2f}", delta=f"{saldo:,.2f}")
        else: st.info("Nenhum lançamento realizado para exibir no dashboard.")
    
    with tab2:
        st.header("Histórico de Lançamentos")
        if transactions_df.empty: st.info("Ainda não há lançamentos registrados.")
        else:
            header_cols = st.columns([3, 1, 1, 1, 2]); header_cols[0].write("**Descrição**"); header_cols[1].write("**Valor**"); header_cols[2].write("**Data Pag.**"); header_cols[3].write("**Status**"); header_cols[4].write("**Ações**"); st.markdown("---")
            for index, row in transactions_df.iterrows():
                col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 2]);
                with col1:
                    st.write(row["description"]); contact_display = row['contact_name'] or 'Nenhum'; st.caption(f"Categoria: {row['category']} | Contato: {contact_display}")
                with col2: st.write(f"US$ {row['amount']:,.2f}")
                with col3: st.write(row["payment_date"])
                with col4: st.write(row["status"])
                with col5:
                    if st.session_state.transaction_to_delete == row['id']:
                        st.warning("Deletar?"); confirm_col, cancel_col = st.columns(2)
                        if confirm_col.button("Sim", key=f"confirm_{row['id']}"): asyncio.run(delete_transaction(row['id'])); st.session_state.transaction_to_delete = None; st.rerun()
                        if cancel_col.button("Não", key=f"cancel_{row['id']}"): st.session_state.transaction_to_delete = None; st.rerun()
                    else:
                        if st.button("Deletar", key=f"delete_{row['id']}"): st.session_state.transaction_to_delete = row['id']; st.rerun()
    
    with tab3:
        st.header("Gestão de Reembolsos"); st.subheader("Pendentes")
        pending_refunds = refunds_df[refunds_df['status'] == 'Pendente']
        if pending_refunds.empty: st.info("Nenhum reembolso pendente.")
        else:
            for _, row in pending_refunds.iterrows():
                with st.container(border=True):
                    st.write(f"**Transação:** {row['description']} | **Valor:** US$ {row['amount']:,.2f}")
                    current_index = 0
                    if row['refund_to_contact_id'] and not pd.isna(row['refund_to_name']):
                        try: current_index = contact_names.index(row['refund_to_name'])
                        except (ValueError, KeyError): current_index = 0
                    selected_refund_contact = st.selectbox("Reembolsar para:", options=contact_names, index=current_index, key=f"refund_to_{row['refund_id']}")
                    col1, col2 = st.columns([1,3])
                    if col1.button("Salvar Contato", key=f"save_name_{row['refund_id']}"):
                        contact_id = int(contacts_df[contacts_df['name'] == selected_refund_contact]['id'].iloc[0]) if selected_refund_contact != "-- Nenhum --" else None
                        asyncio.run(update_refund_contact(contact_id, row['refund_id'])); st.rerun()
                    if col2.button("✅ Marcar como Feito", key=f"mark_done_{row['refund_id']}", type="primary"):
                        asyncio.run(mark_refund_as_done(row['refund_id'])); st.rerun()
        with st.expander("Ver Reembolsos Concluídos"):
            completed_refunds = refunds_df[refunds_df['status'] == 'Concluído']
            st.dataframe(completed_refunds[['description', 'amount', 'refund_to_name']], use_container_width=True)

    with tab4:
        st.header("Gestão de Contatos")
        with st.form(key="contact_form", clear_on_submit=True):
            st.subheader("Adicionar Novo Contato"); col1, col2 = st.columns(2); name = col1.text_input("Nome*"); doc = col2.text_input("Documento (CPF/CNPJ)"); ctype = col1.selectbox("Tipo", ["Cliente", "Fornecedor", "Sócio","Outro"]); notes = st.text_area("Notas"); add_contact_button = st.form_submit_button("Adicionar Contato")
            if add_contact_button:
                if name:
                    asyncio.run(add_new_contact(name, doc, ctype, notes))
                    st.rerun()
                else:
                    st.error("O campo 'Nome' é obrigatório.")
        st.markdown("---"); st.subheader("Lista de Contatos"); st.dataframe(contacts_df, use_container_width=True)

    st.sidebar.header("Exportar Dados")
    if not transactions_df.empty:
        excel_data = convert_dfs_to_excel(transactions_df, refunds_df, contacts_df)
        st.sidebar.download_button(
            label="📥 Exportar Relatório Completo",
            data=excel_data,
            file_name=f"relatorio_completo_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.sidebar.info("Nenhum dado para exportar.")


if __name__ == "__main__":
    main()
