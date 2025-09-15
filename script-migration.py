#!/usr/bin/env python3
"""
Script de Migração: PostgreSQL para MariaDB
Companies → Queues | Tickets → Tickets | Messages → Messages

Autor: Sistema de Migração
Data: 2025-09-15
"""

import psycopg2
import mysql.connector
import json
import logging
from datetime import datetime
import sys
import traceback

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatabaseMigration:
    def __init__(self):
        # Configurações PostgreSQL
        self.pg_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'thefloridalounge',
            'user': 'thefloridalounge',
            'password': 'mudar123'
        }
        
        # Configurações MariaDB
        self.mysql_config = {
            'host': 'localhost',  # Ajuste conforme necessário
            'port': 3307,
            'database': 'whaticket',
            'user': 'whaticket',
            'password': 'whaticket'
        }
        
        self.pg_conn = None
        self.mysql_conn = None
        self.backup_data = {}
        
    def connect_databases(self):
        """Conecta aos bancos de dados"""
        try:
            # Conexão PostgreSQL
            self.pg_conn = psycopg2.connect(**self.pg_config)
            self.pg_conn.autocommit = False
            logger.info("✅ Conectado ao PostgreSQL")
            
            # Conexão MariaDB
            self.mysql_conn = mysql.connector.connect(**self.mysql_config)
            self.mysql_conn.autocommit = False
            logger.info("✅ Conectado ao MariaDB")
            
        except Exception as e:
            logger.error(f"❌ Erro ao conectar aos bancos: {e}")
            raise
    
    def disconnect_databases(self):
        """Desconecta dos bancos de dados"""
        if self.pg_conn:
            self.pg_conn.close()
            logger.info("🔌 Desconectado do PostgreSQL")
        if self.mysql_conn:
            self.mysql_conn.close()
            logger.info("🔌 Desconectado do MariaDB")
    
    def backup_existing_data(self):
        """Faz backup dos dados existentes no MariaDB para rollback"""
        logger.info("📦 Fazendo backup dos dados existentes...")
        
        try:
            cursor = self.mysql_conn.cursor()
            
            # Backup Messages
            cursor.execute("SELECT * FROM Messages")
            self.backup_data['messages'] = cursor.fetchall()
            logger.info(f"📦 Backup Messages: {len(self.backup_data['messages'])} registros")
            
            # Backup Tickets
            cursor.execute("SELECT * FROM Tickets")
            self.backup_data['tickets'] = cursor.fetchall()
            logger.info(f"📦 Backup Tickets: {len(self.backup_data['tickets'])} registros")
            
            # Backup Queues
            cursor.execute("SELECT * FROM Queues")
            self.backup_data['queues'] = cursor.fetchall()
            logger.info(f"📦 Backup Queues: {len(self.backup_data['queues'])} registros")
            
            # Backup Contacts
            cursor.execute("SELECT * FROM Contacts")
            self.backup_data['contacts'] = cursor.fetchall()
            logger.info(f"📦 Backup Contacts: {len(self.backup_data['contacts'])} registros")
            
            # Backup Users
            cursor.execute("SELECT * FROM Users")
            self.backup_data['users'] = cursor.fetchall()
            logger.info(f"📦 Backup Users: {len(self.backup_data['users'])} registros")
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"❌ Erro no backup: {e}")
            raise
    
    def clear_target_tables(self):
        """Limpa as tabelas de destino no MariaDB"""
        logger.info("🧹 Limpando tabelas de destino...")
        
        try:
            cursor = self.mysql_conn.cursor()
            
            # Desabilitar foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # Limpar tabelas na ordem correta (respeitando FK)
            tables_to_clear = ['Messages', 'Tickets', 'UserQueues', 'WhatsappQueues', 'Users', 'Contacts', 'Queues']
            
            for table in tables_to_clear:
                cursor.execute(f"DELETE FROM {table}")
                affected = cursor.rowcount
                logger.info(f"🧹 Limpou {table}: {affected} registros removidos")
            
            # Resetar AUTO_INCREMENT
            cursor.execute("ALTER TABLE Queues AUTO_INCREMENT = 1")
            cursor.execute("ALTER TABLE Tickets AUTO_INCREMENT = 1")
            cursor.execute("ALTER TABLE Contacts AUTO_INCREMENT = 1")
            cursor.execute("ALTER TABLE Users AUTO_INCREMENT = 1")
            
            # Reabilitar foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"❌ Erro ao limpar tabelas: {e}")
            raise
    
    def migrate_companies_to_queues(self):
        """Migra Companies do PostgreSQL para Queues no MariaDB"""
        logger.info("🏢 Migrando Companies → Queues...")
        
        try:
            # Buscar companies do PostgreSQL
            pg_cursor = self.pg_conn.cursor()
            pg_cursor.execute('''
                SELECT id, name, "createdAt", "updatedAt", schedules
                FROM "Companies" 
                WHERE status = true
                ORDER BY id
            ''')
            companies = pg_cursor.fetchall()
            
            logger.info(f"📊 Encontradas {len(companies)} companies para migrar")
            
            # Inserir como filas no MariaDB
            mysql_cursor = self.mysql_conn.cursor()
            
            colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#A8E6CF']
            
            for i, company in enumerate(companies):
                company_id, name, created_at, updated_at, schedules = company
                
                # Converter schedules JSONB para texto
                schedules_text = json.dumps(schedules) if schedules else '[]'
                
                # Escolher cor baseada no índice
                color = colors[i % len(colors)]
                
                mysql_cursor.execute('''
                    INSERT INTO Queues (id, name, color, greetingMessage, createdAt, updatedAt, schedules, outOfHoursMessage)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    company_id,
                    f"Fila: {name}",
                    color,
                    f"Bem-vindo à {name}! Como podemos ajudá-lo?",
                    created_at,
                    updated_at,
                    schedules_text,
                    "Estamos fora do horário de atendimento. Deixe sua mensagem que retornaremos em breve."
                ))
                
                logger.info(f"✅ Company '{name}' → Queue ID {company_id}")
            
            pg_cursor.close()
            mysql_cursor.close()
            
            logger.info(f"✅ Migração Companies → Queues concluída: {len(companies)} registros")
            
        except Exception as e:
            logger.error(f"❌ Erro na migração Companies → Queues: {e}")
            raise
    
    def migrate_contacts(self):
        """Migra Contacts do PostgreSQL para MariaDB"""
        logger.info("👥 Migrando Contacts...")
        
        try:
            pg_cursor = self.pg_conn.cursor()
            pg_cursor.execute('''
                SELECT id, name, number, "profilePicUrl", "createdAt", "updatedAt", email, "isGroup"
                FROM "Contacts"
                WHERE "companyId" IS NOT NULL
                ORDER BY id
            ''')
            contacts = pg_cursor.fetchall()
            
            logger.info(f"📊 Encontrados {len(contacts)} contacts para migrar")
            
            mysql_cursor = self.mysql_conn.cursor()
            
            for contact in contacts:
                contact_id, name, number, profile_pic, created_at, updated_at, email, is_group = contact
                
                mysql_cursor.execute('''
                    INSERT INTO Contacts (id, name, number, profilePicUrl, createdAt, updatedAt, email, isGroup)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    contact_id,
                    name,
                    number,
                    profile_pic,
                    created_at,
                    updated_at,
                    email or '',
                    is_group
                ))
            
            pg_cursor.close()
            mysql_cursor.close()
            
            logger.info(f"✅ Migração Contacts concluída: {len(contacts)} registros")
            
        except Exception as e:
            logger.error(f"❌ Erro na migração Contacts: {e}")
            raise
    
    def migrate_users(self):
        """Migra Users do PostgreSQL para MariaDB"""
        logger.info("👤 Migrando Users...")
        
        try:
            pg_cursor = self.pg_conn.cursor()
            pg_cursor.execute('''
                SELECT id, name, email, "passwordHash", "createdAt", "updatedAt", profile, "tokenVersion", online
                FROM "Users"
                WHERE "companyId" IS NOT NULL
                ORDER BY id
            ''')
            users = pg_cursor.fetchall()
            
            logger.info(f"📊 Encontrados {len(users)} users para migrar")
            
            mysql_cursor = self.mysql_conn.cursor()
            
            for user in users:
                user_id, name, email, password_hash, created_at, updated_at, profile, token_version, online = user
                
                mysql_cursor.execute('''
                    INSERT INTO Users (id, name, email, passwordHash, createdAt, updatedAt, profile, tokenVersion, whatsappId, online)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    user_id,
                    name,
                    email,
                    password_hash,
                    created_at,
                    updated_at,
                    profile,
                    token_version,
                    None,  # whatsappId será nulo inicialmente
                    online
                ))
            
            pg_cursor.close()
            mysql_cursor.close()
            
            logger.info(f"✅ Migração Users concluída: {len(users)} registros")
            
        except Exception as e:
            logger.error(f"❌ Erro na migração Users: {e}")
            raise
    
    def migrate_tickets(self):
        """Migra Tickets do PostgreSQL para MariaDB, associando à fila correta"""
        logger.info("🎫 Migrando Tickets...")
        
        try:
            pg_cursor = self.pg_conn.cursor()
            pg_cursor.execute('''
                SELECT t.id, t.status, t."lastMessage", t."contactId", t."userId", 
                       t."createdAt", t."updatedAt", t."whatsappId", t."isGroup", 
                       t."unreadMessages", t."companyId"
                FROM "Tickets" t
                WHERE t."companyId" IS NOT NULL AND t."contactId" IS NOT NULL
                ORDER BY t.id
            ''')
            tickets = pg_cursor.fetchall()
            
            logger.info(f"📊 Encontrados {len(tickets)} tickets para migrar")
            
            mysql_cursor = self.mysql_conn.cursor()
            
            for ticket in tickets:
                ticket_id, status, last_message, contact_id, user_id, created_at, updated_at, whatsapp_id, is_group, unread_messages, company_id = ticket
                
                # A company_id vira a queueId no MariaDB
                mysql_cursor.execute('''
                    INSERT INTO Tickets (id, status, lastMessage, contactId, userId, createdAt, updatedAt, whatsappId, isGroup, unreadMessages, queueId)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    ticket_id,
                    status,
                    last_message,
                    contact_id,
                    user_id,
                    created_at,
                    updated_at,
                    whatsapp_id,
                    is_group,
                    unread_messages,
                    company_id  # company_id vira queueId
                ))
            
            pg_cursor.close()
            mysql_cursor.close()
            
            logger.info(f"✅ Migração Tickets concluída: {len(tickets)} registros")
            
        except Exception as e:
            logger.error(f"❌ Erro na migração Tickets: {e}")
            raise
    
    def migrate_messages(self):
        """Migra Messages do PostgreSQL para MariaDB"""
        logger.info("💬 Migrando Messages...")
        
        try:
            pg_cursor = self.pg_conn.cursor()
            pg_cursor.execute('''
                SELECT m.id, m.body, m.ack, m.read, m."mediaType", m."mediaUrl", 
                       m."ticketId", m."createdAt", m."updatedAt", m."fromMe", 
                       m."isDeleted", m."contactId", m."quotedMsgId"
                FROM "Messages" m
                INNER JOIN "Tickets" t ON m."ticketId" = t.id
                WHERE t."companyId" IS NOT NULL
                ORDER BY m."createdAt"
            ''')
            messages = pg_cursor.fetchall()
            
            logger.info(f"📊 Encontradas {len(messages)} messages para migrar")
            
            mysql_cursor = self.mysql_conn.cursor()
            
            batch_size = 1000
            total_batches = (len(messages) + batch_size - 1) // batch_size
            
            for i in range(0, len(messages), batch_size):
                batch = messages[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                logger.info(f"📦 Processando batch {batch_num}/{total_batches} ({len(batch)} messages)")
                
                for message in batch:
                    msg_id, body, ack, read, media_type, media_url, ticket_id, created_at, updated_at, from_me, is_deleted, contact_id, quoted_msg_id = message
                    
                    mysql_cursor.execute('''
                        INSERT INTO Messages (id, body, ack, `read`, mediaType, mediaUrl, ticketId, createdAt, updatedAt, fromMe, isDeleted, contactId, quotedMsgId)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        msg_id,
                        body,
                        ack,
                        read,
                        media_type,
                        media_url,
                        ticket_id,
                        created_at,
                        updated_at,
                        from_me,
                        is_deleted,
                        contact_id,
                        quoted_msg_id
                    ))
            
            pg_cursor.close()
            mysql_cursor.close()
            
            logger.info(f"✅ Migração Messages concluída: {len(messages)} registros")
            
        except Exception as e:
            logger.error(f"❌ Erro na migração Messages: {e}")
            raise
    
    def validate_migration(self):
        """Valida a migração comparando contadores"""
        logger.info("🔍 Validando migração...")
        
        try:
            # Contar registros no PostgreSQL
            pg_cursor = self.pg_conn.cursor()
            
            pg_cursor.execute('SELECT COUNT(*) FROM "Companies" WHERE status = true')
            pg_companies = pg_cursor.fetchone()[0]
            
            pg_cursor.execute('SELECT COUNT(*) FROM "Tickets" WHERE "companyId" IS NOT NULL')
            pg_tickets = pg_cursor.fetchone()[0]
            
            pg_cursor.execute('''
                SELECT COUNT(*) FROM "Messages" m 
                INNER JOIN "Tickets" t ON m."ticketId" = t.id 
                WHERE t."companyId" IS NOT NULL
            ''')
            pg_messages = pg_cursor.fetchone()[0]
            
            # Contar registros no MariaDB
            mysql_cursor = self.mysql_conn.cursor()
            
            mysql_cursor.execute('SELECT COUNT(*) FROM Queues')
            mysql_queues = mysql_cursor.fetchone()[0]
            
            mysql_cursor.execute('SELECT COUNT(*) FROM Tickets')
            mysql_tickets = mysql_cursor.fetchone()[0]
            
            mysql_cursor.execute('SELECT COUNT(*) FROM Messages')
            mysql_messages = mysql_cursor.fetchone()[0]
            
            # Validar contadores
            logger.info("📊 VALIDAÇÃO DE MIGRAÇÃO:")
            logger.info(f"   Companies → Queues: {pg_companies} → {mysql_queues} {'✅' if pg_companies == mysql_queues else '❌'}")
            logger.info(f"   Tickets: {pg_tickets} → {mysql_tickets} {'✅' if pg_tickets == mysql_tickets else '❌'}")
            logger.info(f"   Messages: {pg_messages} → {mysql_messages} {'✅' if pg_messages == mysql_messages else '❌'}")
            
            # Verificar integridade de FKs
            mysql_cursor.execute('''
                SELECT COUNT(*) FROM Tickets t 
                LEFT JOIN Queues q ON t.queueId = q.id 
                WHERE t.queueId IS NOT NULL AND q.id IS NULL
            ''')
            orphaned_tickets = mysql_cursor.fetchone()[0]
            
            mysql_cursor.execute('''
                SELECT COUNT(*) FROM Messages m 
                LEFT JOIN Tickets t ON m.ticketId = t.id 
                WHERE t.id IS NULL
            ''')
            orphaned_messages = mysql_cursor.fetchone()[0]
            
            logger.info(f"   Tickets órfãos (sem queue): {orphaned_tickets} {'✅' if orphaned_tickets == 0 else '❌'}")
            logger.info(f"   Messages órfãs (sem ticket): {orphaned_messages} {'✅' if orphaned_messages == 0 else '❌'}")
            
            pg_cursor.close()
            mysql_cursor.close()
            
            # Retornar se validação passou
            validation_passed = (
                pg_companies == mysql_queues and
                pg_tickets == mysql_tickets and
                pg_messages == mysql_messages and
                orphaned_tickets == 0 and
                orphaned_messages == 0
            )
            
            return validation_passed
            
        except Exception as e:
            logger.error(f"❌ Erro na validação: {e}")
            return False
    
    def rollback_migration(self):
        """Desfaz a migração restaurando os dados de backup"""
        logger.warning("⏪ Iniciando ROLLBACK da migração...")
        
        try:
            cursor = self.mysql_conn.cursor()
            
            # Desabilitar foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # Limpar dados migrados
            tables = ['Messages', 'Tickets', 'UserQueues', 'WhatsappQueues', 'Users', 'Contacts', 'Queues']
            for table in tables:
                cursor.execute(f"DELETE FROM {table}")
                logger.info(f"🧹 Limpou {table} para rollback")
            
            # Restaurar dados de backup se existirem
            if self.backup_data.get('queues'):
                for queue in self.backup_data['queues']:
                    placeholders = ', '.join(['%s'] * len(queue))
                    cursor.execute(f"INSERT INTO Queues VALUES ({placeholders})", queue)
                logger.info(f"📦 Restaurou {len(self.backup_data['queues'])} Queues")
            
            if self.backup_data.get('contacts'):
                for contact in self.backup_data['contacts']:
                    placeholders = ', '.join(['%s'] * len(contact))
                    cursor.execute(f"INSERT INTO Contacts VALUES ({placeholders})", contact)
                logger.info(f"📦 Restaurou {len(self.backup_data['contacts'])} Contacts")
            
            if self.backup_data.get('users'):
                for user in self.backup_data['users']:
                    placeholders = ', '.join(['%s'] * len(user))
                    cursor.execute(f"INSERT INTO Users VALUES ({placeholders})", user)
                logger.info(f"📦 Restaurou {len(self.backup_data['users'])} Users")
            
            if self.backup_data.get('tickets'):
                for ticket in self.backup_data['tickets']:
                    placeholders = ', '.join(['%s'] * len(ticket))
                    cursor.execute(f"INSERT INTO Tickets VALUES ({placeholders})", ticket)
                logger.info(f"📦 Restaurou {len(self.backup_data['tickets'])} Tickets")
            
            if self.backup_data.get('messages'):
                for message in self.backup_data['messages']:
                    placeholders = ', '.join(['%s'] * len(message))
                    cursor.execute(f"INSERT INTO Messages VALUES ({placeholders})", message)
                logger.info(f"📦 Restaurou {len(self.backup_data['messages'])} Messages")
            
            # Reabilitar foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            cursor.close()
            
            self.mysql_conn.commit()
            logger.warning("⏪ ROLLBACK concluído com sucesso!")
            
        except Exception as e:
            logger.error(f"❌ Erro durante o rollback: {e}")
            raise
    
    def run_migration(self, dry_run=False):
        """Executa a migração completa"""
        try:
            logger.info("🚀 Iniciando migração PostgreSQL → MariaDB")
            
            if dry_run:
                logger.info("🔍 MODO DRY RUN - Apenas validação, sem modificar dados")
            
            # Conectar aos bancos
            self.connect_databases()
            
            if not dry_run:
                # Fazer backup dos dados existentes
                self.backup_existing_data()
                
                # Limpar tabelas de destino
                self.clear_target_tables()
                
                # Executar migrações
                self.migrate_companies_to_queues()
                self.migrate_contacts()
                self.migrate_users()
                self.migrate_tickets()
                self.migrate_messages()
                
                # Validar migração
                validation_passed = self.validate_migration()
                
                if validation_passed:
                    # Commit das transações
                    self.mysql_conn.commit()
                    logger.info("✅ MIGRAÇÃO CONCLUÍDA COM SUCESSO!")
                else:
                    logger.error("❌ Validação falhou - Executando rollback")
                    self.rollback_migration()
                    return False
            else:
                # Apenas mostrar estatísticas em modo dry run
                pg_cursor = self.pg_conn.cursor()
                
                pg_cursor.execute('SELECT COUNT(*) FROM "Companies" WHERE status = true')
                companies_count = pg_cursor.fetchone()[0]
                
                pg_cursor.execute('SELECT COUNT(*) FROM "Tickets" WHERE "companyId" IS NOT NULL')
                tickets_count = pg_cursor.fetchone()[0]
                
                pg_cursor.execute('''
                    SELECT COUNT(*) FROM "Messages" m 
                    INNER JOIN "Tickets" t ON m."ticketId" = t.id 
                    WHERE t."companyId" IS NOT NULL
                ''')
                messages_count = pg_cursor.fetchone()[0]
                
                logger.info("📊 ESTATÍSTICAS DE MIGRAÇÃO (DRY RUN):")
                logger.info(f"   Companies → Queues: {companies_count}")
                logger.info(f"   Tickets: {tickets_count}")
                logger.info(f"   Messages: {messages_count}")
                
                pg_cursor.close()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro durante a migração: {e}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            
            if not dry_run and self.mysql_conn:
                logger.warning("⏪ Executando rollback devido ao erro...")
                self.rollback_migration()
            
            return False
            
        finally:
            self.disconnect_databases()

def main():
    """Função principal"""
    print("=" * 60)
    print("🔄 SCRIPT DE MIGRAÇÃO PostgreSQL → MariaDB")
    print("   Companies → Queues | Tickets + Messages")
    print("=" * 60)
    
    # Perguntar se quer executar em modo dry run
    while True:
        choice = input("\n🔍 Executar em modo DRY RUN primeiro? (s/n): ").lower().strip()
        if choice in ['s', 'sim', 'y', 'yes']:
            dry_run = True
            break
        elif choice in ['n', 'não', 'no', 'nao']:
            dry_run = False
            break
        else:
            print("❌ Resposta inválida. Digite 's' para sim ou 'n' para não.")
    
    # Executar migração
    migration = DatabaseMigration()
    success = migration.run_migration(dry_run=dry_run)
    
    if dry_run and success:
        print("\n" + "=" * 60)
        print("✅ DRY RUN concluído com sucesso!")
        
        while True:
            choice = input("\n🚀 Executar migração real agora? (s/n): ").lower().strip()
            if choice in ['s', 'sim', 'y', 'yes']:
                migration_real = DatabaseMigration()
                success_real = migration_real.run_migration(dry_run=False)
                break
            elif choice in ['n', 'não', 'no', 'nao']:
                print("⏹️  Migração cancelada pelo usuário.")
                break
            else:
                print("❌ Resposta inválida. Digite 's' para sim ou 'n' para não.")
    
    print("\n" + "=" * 60)
    if success:
        print("✅ PROCESSO CONCLUÍDO!")
    else:
        print("❌ PROCESSO FALHOU!")
    print("📋 Verifique o arquivo 'migration.log' para detalhes completos.")
    print("=" * 60)

if __name__ == "__main__":
    main()