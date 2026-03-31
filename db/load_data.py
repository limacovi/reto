import pandas as pd
import os
from db.connection import get_engine
from sqlalchemy import text

PROCESSED_PATH = "processed"

def load_data():
    engine = get_engine()

    with engine.begin() as conn:

        #Debug
        print("DB actual:", conn.execute(text("SELECT current_database();")).fetchone())

        for file in os.listdir(PROCESSED_PATH):
            if file.endswith(".csv"):
                print(f"\n Procesando archivo: {file}")

                df = pd.read_csv(f"{PROCESSED_PATH}/{file}")

                print("Columnas:", df.columns.tolist())
                print("Total filas:", len(df))

                # =========================
                # VALIDACIÓN DE COLUMNAS
                # =========================
                required_cols = ['transaction_id', 'user_id', 'amount', 'timestamp']
                for col in required_cols:
                    if col not in df.columns:
                        raise ValueError(f"Falta columna crítica: {col}")

                # =========================
                # LIMPIEZA BASE
                # =========================
                df = df.drop_duplicates(subset=['transaction_id'])

                # =========================
                # DIM USERS (SIN ON CONFLICT)
                # =========================
                if 'user_id' in df.columns:
                    dim_users = df[['user_id', 'country']].drop_duplicates()

                    for _, row in dim_users.iterrows():
                        conn.execute(
                            text("""
                                INSERT INTO dim_users (user_id, country)
                                SELECT :user_id, :country
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM dim_users WHERE user_id = :user_id
                                );
                            """),
                            {
                                "user_id": str(row['user_id']),
                                "country": row.get('country', 'UNKNOWN')
                            }
                        )
                # =========================
                # DIM MERCHANTS
                # =========================
                if 'merchant_id' in df.columns:
                    dim_merchants = df[['merchant_id']].drop_duplicates()

                    for _, row in dim_merchants.iterrows():
                        conn.execute(
                            text("""
                                INSERT INTO dim_merchants (merchant_id)
                                SELECT :merchant_id
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM dim_merchants WHERE merchant_id = :merchant_id
                                );
                            """),
                            {
                                "merchant_id": str(row['merchant_id'])
                            }
                        )
                # =========================
                # FACT TRANSACTIONS
                # =========================
                for _, row in df.iterrows():
                    conn.execute(
                        text("""
                            INSERT INTO fact_transactions (
                                transaction_id,
                                user_id,
                                amount,
                                timestamp,
                                status,
                                country
                            )
                            SELECT :transaction_id, :user_id, :amount, :timestamp, :status, :country
                            WHERE NOT EXISTS (
                                SELECT 1 FROM fact_transactions WHERE transaction_id = :transaction_id
                            );
                        """),
                        {
                            "transaction_id": str(row['transaction_id']),
                            "user_id": str(row['user_id']),
                            "amount": float(row['amount']),
                            "timestamp": row['timestamp'],
                            "status": row.get('status', 'unknown'),
                            "country": row.get('country', 'UNKNOWN')
                        }
                    )

                print(f" Archivo {file} cargado sin duplicados")

    print("\n Carga finalizada correctamente")


if __name__ == "__main__":
    load_data()