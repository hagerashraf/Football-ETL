import pandas as pd
import logging 
from datetime import datetime
import os
import pyodbc
import schedule
import time

class FootballETL:
    def __init__(self, excel_file_path, connection_string):
        self.excel_file_path = excel_file_path      
        self.connection_string = connection_string 
        self.conn = None                            
        self.cursor = None                          
        self.dataframes = {}                        
        self.load_summary = {}                      
        self.errors_found = []                       
        self.setup_logging()

    def setup_logging(self):
        log_filename = f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        print(f"📄 Logging to: {log_filename}")   #this line informs that we started to log in file
    
    def connect_to_database(self): #  Establish connection to SQL Server database
        try:
            self.conn = pyodbc.connect(self.connection_string)
            self.cursor = self.conn.cursor()
            print("✅ Connected to SQL Server")
            self.logger.info("Connected to SQL Server successfully")
            return True
            
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            self.logger.error(f"Database connection failed: {e}")
            return False
        
    def create_error_table(self):
        create_error_table_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DataQualityErrors' AND xtype='U')
        CREATE TABLE DataQualityErrors (
            ErrorID INT IDENTITY(1,1) PRIMARY KEY,
            TableName VARCHAR(50),
            ErrorType VARCHAR(100),
            ErrorDescription VARCHAR(500),
            ErrorDateTime DATETIME DEFAULT GETDATE()
        )
        """
        try:
            self.cursor.execute(create_error_table_sql)
            self.conn.commit()
            print("✅ Error tracking table ready")
            self.logger.info("Error tracking table created/verified")
            
        except Exception as e:
            print(f"❌ Error table creation failed: {e}")
            self.logger.error(f"Error table creation failed: {e}")
    
    def extract_data(self):
        try:
            print("📥 Extracting data from Excel...")
            self.dataframes['teams'] = pd.read_excel(self.excel_file_path, sheet_name='Teams')
            self.dataframes['players'] = pd.read_excel(self.excel_file_path, sheet_name='Players')
            self.dataframes['matches'] = pd.read_excel(self.excel_file_path, sheet_name='Matches')
            self.dataframes['playerstats'] = pd.read_excel(self.excel_file_path, sheet_name='PlayerStats')
            self.dataframes['transfers'] = pd.read_excel(self.excel_file_path, sheet_name='PlayerTransfers')
            
            for table_name, df in self.dataframes.items():
                count = len(df)
                print(f"   {table_name.title()}: {count} records extracted")
                self.logger.info(f"Extracted {count} records from {table_name}")
            
            print("✅ Data extraction completed")
            return True
            
        except Exception as e:
            print(f"❌ Data extraction failed: {e}")
            self.logger.error(f"Data extraction failed: {e}")
            return False

    def clean_foreign_keys(self):
        print("🔍 Cleaning invalid foreign keys...")
        # Get valid IDs
        valid_team_ids = set(self.dataframes['teams']['TeamID'])
        valid_player_ids = set(self.dataframes['players']['PlayerID'])
        valid_match_ids = set(self.dataframes['matches']['MatchID'])
        
        # Clean PlayerStats (remove records with invalid PlayerID or MatchID)
        ps_before = len(self.dataframes['playerstats'])
        self.dataframes['playerstats'] = self.dataframes['playerstats'][
            (self.dataframes['playerstats']['PlayerID'].isin(valid_player_ids)) &
            (self.dataframes['playerstats']['MatchID'].isin(valid_match_ids))
        ]
        ps_removed = ps_before - len(self.dataframes['playerstats'])
        
        # Clean PlayerTransfers (remove records with invalid PlayerID, FromTeamID, ToTeamID)
        pt_before = len(self.dataframes['transfers'])
        self.dataframes['transfers'] = self.dataframes['transfers'][
            (self.dataframes['transfers']['PlayerID'].isin(valid_player_ids)) &
            (self.dataframes['transfers']['FromTeamID'].isin(valid_team_ids)) &
            (self.dataframes['transfers']['ToTeamID'].isin(valid_team_ids))
        ]
        pt_removed = pt_before - len(self.dataframes['transfers'])
        
        # Clean Players (remove records with invalid TeamID)
        p_before = len(self.dataframes['players'])
        self.dataframes['players'] = self.dataframes['players'][
            self.dataframes['players']['TeamID'].isin(valid_team_ids)
        ]
        p_removed = p_before - len(self.dataframes['players'])
        
        # Clean Matches (remove records with invalid HomeTeamID or AwayTeamID)
        m_before = len(self.dataframes['matches'])
        self.dataframes['matches'] = self.dataframes['matches'][
            (self.dataframes['matches']['HomeTeamID'].isin(valid_team_ids)) &
            (self.dataframes['matches']['AwayTeamID'].isin(valid_team_ids))
        ]
        m_removed = m_before - len(self.dataframes['matches'])
        
        # Log any removals
        if ps_removed > 0:
            self.errors_found.append(('PlayerStats', 'Invalid Foreign Key', f'{ps_removed} records with invalid PlayerID/MatchID removed'))
            print(f"   🗑️ Removed {ps_removed} Invalid PlayerStats records")
            
        if pt_removed > 0:
            self.errors_found.append(('PlayerTransfers', 'Invalid Foreign Key', f'{pt_removed} records with invalid PlayerID/TeamID removed'))
            print(f"   🗑️ Removed {pt_removed} Invalid PlayerTransfers records")
            
        if p_removed > 0:
            self.errors_found.append(('Players', 'Invalid Foreign Key', f'{p_removed} records with invalid TeamID removed'))
            print(f"   🗑️ Removed {p_removed} Invalid Players records")
            
        if m_removed > 0:
            self.errors_found.append(('Matches', 'Invalid Foreign Key', f'{m_removed} records with invalid TeamID removed'))
            print(f"   🗑️ Removed {m_removed} Invalid Matches records")
        
        total_removed = ps_removed + pt_removed + p_removed + m_removed
        print(f"✅ Foreign key cleanup completed. Total Invalid records removed: {total_removed}")
        
        return True
    
    def clean_teams_data(self):
        df = self.dataframes['teams']
        original_count = len(df)
        
        df.drop_duplicates(subset=['TeamID'], inplace=True)
        
        missing_data = df[df['TeamName'].isna() | df['Country'].isna()]
        if not missing_data.empty:
            self.errors_found.append(('Teams', 'Missing Data', f'{len(missing_data)} teams with missing name/country'))
        
        self.dataframes['teams'] = df
        print(f"   Teams: {original_count} → {len(df)} records")
        return len(df)    
    
    def clean_players_data(self):
        df = self.dataframes['players']
        original_count = len(df)
        df.drop_duplicates(subset=['PlayerID'], inplace=True)
        # Fix expired contracts
        df['ContractUntil'] = pd.to_datetime(df['ContractUntil'])
        current_date = datetime.now()
        expired_contracts = df['ContractUntil'] < current_date
        
        if expired_contracts.sum() > 0:
            self.errors_found.append(('Players', 'Expired Contracts', f'{expired_contracts.sum()} players with expired contracts'))
 
        self.dataframes['players'] = df
        print(f"   Players: {original_count} → {len(df)} records")
        return len(df)

    def clean_playerstats_data(self):
        df = self.dataframes['playerstats']
        original_count = len(df)
        # Add missing Assists column if it doesn't exist
        if 'Assists' not in df.columns:
            print("Adding missing Assists column...")
            df['Assists']=0
        # Red cards should be maximum 1 per match
        invalid_red_cards = df['RedCards'] > 1
        if invalid_red_cards.sum() > 0:
            self.errors_found.append(('PlayerStats', 'Invalid Red Cards', f'{invalid_red_cards.sum()} records with >1 red card'))
            df.loc[invalid_red_cards, 'RedCards'] = 1
        
        self.dataframes['playerstats'] = df
        print(f"   PlayerStats: {original_count} → {len(df)} records")
        return len(df)

    def clean_matches_data(self):
        df = self.dataframes['matches']
        original_count = len(df)
        df.drop_duplicates(subset=['MatchID'], inplace=True)
        # Remove matches where team plays against itself
        same_team_matches = df['HomeTeamID'] == df['AwayTeamID']
        if same_team_matches.sum() > 0:
            self.errors_found.append(('Matches', 'Same Team Match', f'{same_team_matches.sum()} matches where team plays itself'))
            df = df[~same_team_matches]
        
        self.dataframes['matches'] = df
        print(f"   Matches: {original_count} → {len(df)} records")
        return len(df)
    
    def clean_transfers_data(self):
        df = self.dataframes['transfers']
        original_count = len(df)
        df.drop_duplicates(subset=['TransferID'], inplace=True)
        
        same_team_transfers = df['FromTeamID'] == df['ToTeamID']
        if same_team_transfers.sum() > 0:
            self.errors_found.append(('PlayerTransfers', 'Same Team Transfer', f'{same_team_transfers.sum()} transfers to same team'))
            df = df[~same_team_transfers]
        
        self.dataframes['transfers'] = df
        print(f"   PlayerTransfers: {original_count} → {len(df)} records")
        return len(df)

    def transform_data(self):
        print("🧹 Starting data cleaning and transformation...")
        self.logger.info("Starting data transformation process")
        # Step 1: Clean each table's data first
        self.clean_teams_data()
        self.clean_players_data()
        self.clean_playerstats_data()
        self.clean_matches_data()
        self.clean_transfers_data()
        
        # Step 2: Clean invalid foreign keys
        self.clean_foreign_keys()

        # Log all errors found during cleaning
        for error in self.errors_found:
            self.logger.warning(f"Data Quality Issue - {error[0]}: {error[1]} - {error[2]}")
            # Insert error into database
            try:
                insert_error_sql = """
                INSERT INTO DataQualityErrors (TableName, ErrorType, ErrorDescription)
                VALUES (?, ?, ?)
                """
                self.cursor.execute(insert_error_sql, error[0], error[1], error[2])
            except Exception as e:
                self.logger.error(f"Failed to log error to database: {e}")
        
        self.conn.commit()
        print(f"✅ Data transformation completed. {len(self.errors_found)} issues found and logged")
        self.logger.info(f"Data transformation completed. {len(self.errors_found)} issues found")
        
        return True
    
    def load_table_data(self, table_name, dataframe):
        try:
            expected_count = len(dataframe)
            self.cursor.execute(f"DELETE FROM {table_name}")
            self.cursor.execute(f"SET IDENTITY_INSERT {table_name} ON")
            
            # Insert data row by row
            for index, row in dataframe.iterrows():
                # Create dynamic INSERT statement
                columns = ', '.join(row.index)
                placeholders = ', '.join(['?' for _ in row.index])
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                values = [None if pd.isna(value) else value for value in row.values]
                self.cursor.execute(sql, tuple(values))
            
            self.cursor.execute(f"SET IDENTITY_INSERT {table_name} OFF")
            self.conn.commit()
            
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            actual_count = self.cursor.fetchone()[0]
            
            success = expected_count == actual_count
            self.load_summary[table_name] = {
                'expected': expected_count,
                'actual': actual_count,
                'success': success
            }
            
            status = "✅" if success else "⚠️"
            print(f"   {status} {table_name}: {actual_count}/{expected_count} records loaded")
            self.logger.info(f"{table_name} loaded: {actual_count}/{expected_count} records")
            return success
            
        except Exception as e:
            print(f"   ❌ {table_name}: Load failed - {e}")
            self.logger.error(f"{table_name} load failed: {e}")
            
            self.load_summary[table_name] = {
                'expected': len(dataframe),
                'actual': 0,
                'success': False,
                'error': str(e)
            }
            return False
        
    def load_data(self):
        print("📥 Loading data to SQL Server...")
        self.logger.info("Starting data load to SQL Server")
        
        try:
            print("   🔓 Disabling foreign key constraints...")
            self.cursor.execute("EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'")
            self.conn.commit()
            
            table_mappings = [
                ('Teams', 'teams'),
                ('Players', 'players'),
                ('Matches', 'matches'),
                ('PlayerStats', 'playerstats'),
                ('PlayerTransfers', 'transfers')
            ]
            
            all_success = True
            for sql_table, df_key in table_mappings:
                success = self.load_table_data(sql_table, self.dataframes[df_key])
                if not success:
                    all_success = False
            
            # Re-enable foreign key constraints
            print("   🔒 Re-enabling foreign key constraints...")
            self.cursor.execute("EXEC sp_msforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all'")
            self.conn.commit()
            
            print("✅ Data loading completed" if all_success else "⚠️ Data loading completed with errors")
            return all_success
            
        except Exception as e:
            print(f"❌ Error during data loading: {e}")
            self.logger.error(f"Data loading failed: {e}")
            # Try to re-enable constraints even if loading failed
            try:
                self.cursor.execute("EXEC sp_msforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all'")
                self.conn.commit()
            except:
                pass
            return False
    
    def create_denormalized_view(self):
        """Create the denormalized view for reporting"""
        view_sql = """
        CREATE OR ALTER VIEW PlayerDenormalizedView AS
        SELECT 
            p.PlayerID,
            p.Name as PlayerName,
            t.TeamName as CurrentTeam,
            COALESCE(SUM(ps.Goals), 0) as TotalGoals,
            COALESCE(SUM(ps.Assists), 0) as TotalAssists,
            COALESCE(ROUND(AVG(CAST(ps.MinutesPlayed AS FLOAT)), 2), 0) as AverageMinutesPlayed,
            CASE WHEN COALESCE(SUM(ps.MinutesPlayed), 0) > 300 THEN 1 ELSE 0 END as PlayedOver300Min,
            CASE 
                WHEN DATEDIFF(YEAR, p.DateOfBirth, GETDATE()) BETWEEN 25 AND 30 
                THEN 1 ELSE 0 
            END as AgeBetween25And30,
            CASE WHEN MAX(ps.Goals) >= 3 THEN 1 ELSE 0 END as Scored3PlusGoalsInMatch,
            CONCAT(
                COALESCE(SUM(ps.MinutesPlayed), 0) / 90, 
                ' match ', 
                COALESCE(SUM(ps.MinutesPlayed), 0) % 90, 
                ' min'
            ) as EstimatedMatchesPlayed,
            CASE WHEN EXISTS (
                SELECT 1 FROM Teams t2 WHERE t2.Country = 'France' AND t2.TeamID = p.TeamID
            ) THEN 1 ELSE 0 END as PlayedInFrance,
            (SELECT MIN(pt.TransferDate) 
             FROM PlayerTransfers pt 
             JOIN Teams t3 ON pt.ToTeamID = t3.TeamID 
             WHERE pt.PlayerID = p.PlayerID AND t3.Country = 'France'
            ) as DateJoinedFrenchTeam,
            CASE WHEN EXISTS (
                SELECT 1 FROM Teams t4 WHERE t4.Country = 'Germany' AND t4.TeamID = p.TeamID
            ) THEN 1 ELSE 0 END as PlayedInGermany,
            (SELECT MIN(pt.TransferDate) 
             FROM PlayerTransfers pt 
             JOIN Teams t5 ON pt.ToTeamID = t5.TeamID 
             WHERE pt.PlayerID = p.PlayerID AND t5.Country = 'Germany'
            ) as DateJoinedGermanTeam
        FROM Players p
        LEFT JOIN Teams t ON p.TeamID = t.TeamID
        LEFT JOIN PlayerStats ps ON p.PlayerID = ps.PlayerID
        GROUP BY p.PlayerID, p.Name, t.TeamName, p.DateOfBirth, p.TeamID
        """

        
        try:
            self.cursor.execute(view_sql)
            self.conn.commit()
            print("✅ Denormalized view created successfully")
            self.logger.info("Denormalized view created successfully")
            return True
            
        except Exception as e:
            print(f"❌ View creation failed: {e}")
            self.logger.error(f"View creation failed: {e}")
            return False  

    def generate_final_report(self):
        print("\n" + "="*60)
        print("ETL PROCESS FINAL REPORT")
        print("="*60)
        
        # Table load summary
        for table_name, summary in self.load_summary.items():
            status = "SUCCESS" if summary['success'] else "FAILED"
            print(f"{table_name:15}: {summary['actual']:3}/{summary['expected']:3} records - {status}")
            
            # Write to log
            self.logger.info(f"FINAL: {table_name} - Expected: {summary['expected']}, Actual: {summary['actual']}, Status: {status}")
        
        # Data quality summary
        print(f"\nData Quality Issues Found: {len(self.errors_found)}")
        
        # Overall status
        all_success = all(summary['success'] for summary in self.load_summary.values())
        overall_status = "SUCCESS" if all_success else "PARTIAL SUCCESS"
        print(f"Overall ETL Status: {overall_status}")
        
        self.logger.info(f"ETL Process completed with status: {overall_status}")
        
        return overall_status
    
    def run_etl(self):
        print("🚀 Starting Football Database ETL Process...")
        start_time = datetime.now()
        
        try:
            # Step 1: Connect to database
            if not self.connect_to_database():
                return False
            
            # Step 2: Create error tracking table
            self.create_error_table()
            
            # Step 3: Extract data from Excel
            if not self.extract_data():
                return False
            
            # Step 4: Transform and clean data (includes foreign key validation)
            if not self.transform_data():
                return False
            
            # Step 5: Load data to SQL Server
            if not self.load_data():
                print("⚠️ Some tables failed to load completely")
            
            # Step 6: Create denormalized view
            self.create_denormalized_view()
            
            # Step 7: Generate final report
            status = self.generate_final_report()
            
            # Calculate total time
            end_time = datetime.now()
            duration = end_time - start_time
            print(f"\n⏱️ Total ETL Time: {duration}")
            
            print("✅ ETL Process Completed!")
            self.logger.info(f"ETL Process completed successfully in {duration}")
            
            return True
            
        except Exception as e:
            print(f"❌ ETL Process failed: {e}")
            self.logger.error(f"ETL Process failed: {e}")
            return False
        
        finally:
            
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()


    def schedule_etl_job(self):
        try:
            print(f"🚀 Starting scheduled ETL job at {datetime.now()}")
            self.logger.info(f"Starting scheduled ETL job at {datetime.now()}")
            
            # Run the ETL process
            success = self.run_etl()
            if success:
                print("✅ Scheduled ETL job completed successfully")
                self.logger.info("Scheduled ETL job completed successfully")
            else:
                print("❌ Scheduled ETL job failed")
                self.logger.error("Scheduled ETL job failed")
                
        except Exception as e:
            print(f"❌ Error in scheduled ETL job: {e}")
            self.logger.error(f"Error in scheduled ETL job: {e}")

    def start_scheduler(self, schedule_time="02:00"):
        print("ETL Scheduler Started...")
        self.logger.info("ETL Scheduler Started")
        
        # Schedule ETL to run daily at specified time
        schedule.every().day.at(schedule_time).do(self.schedule_etl_job)
        
        print(f"📅 ETL scheduled to run daily at {schedule_time}")
        self.logger.info(f"ETL scheduled to run daily at {schedule_time}")
        
        print("⏰ Scheduler is running... Press Ctrl+C to stop")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            print("\n⏹️ Scheduler stopped by user")
            self.logger.info("Scheduler stopped by user")
        except Exception as e:
            print(f"❌ Scheduler error: {e}")
            self.logger.error(f"Scheduler error: {e}")

    

if __name__ == "__main__":
    # Configuration
    excel_file = "FootballDummyData.xlsx"
    
    connection_string = """
        DRIVER={SQL Server};
        SERVER=DESKTOP-2D9D3TL;
        DATABASE=Final;
        Trusted_Connection=yes
        UID=WW930/a922511;
        PWD=Abdulrahman187*;
    """
    
    etl = FootballETL(excel_file, connection_string)
    success = etl.run_etl()
    
    if success:
        print("🎉 ETL completed successfully!")

        schedule_future = input("\nWould you like to schedule daily runs? (y/n): ").strip().lower()
        
        if schedule_future == 'y':
            schedule_time = input("Enter time (HH:MM) or press Enter for 02:00: ").strip()
            if not schedule_time:
                schedule_time = "02:00"
            
            print(f"\n🔄 Starting scheduler for daily runs at {schedule_time}")
            etl.start_scheduler(schedule_time)
        else:
            print("👋 ETL completed. Program ending.")
    else:
        print("💥 ETL failed. Check logs for details.")