
import sys
import os
import csv
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from indexes.core.database_manager import DatabaseManager
from indexes.core.record import Table, Record

def create_sales_table():
    sql_fields = [
        ("sale_id", "INT", 4),
        ("product_name", "CHAR", 50),
        ("quantity", "INT", 4),
        ("unit_price", "FLOAT", 4),
        ("sale_date", "CHAR", 20)
    ]
    
    return Table(
        table_name="sales",
        sql_fields=sql_fields,
        key_field="sale_id"
    )

def load_sales_from_csv(db_manager, table_name, csv_path, max_records=100):
    
    print(f"\n{'='*60}")
    print(f"LOADING DATA FROM CSV: {csv_path}")
    print(f"{'='*60}")
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';')
        count = 0
        
        for row in reader:
            if count >= max_records:
                break
            
            try:
                sale_id = int(row['ID de la venta'])
                product_name = row['Nombre producto'].strip().encode('utf-8')
                quantity = int(row['Cantidad vendida'])
                unit_price = float(row['Precio unitario'].replace(',', '.'))
                sale_date = row['Fecha de venta'].strip().encode('utf-8')
                
                table_info = db_manager.tables[table_name]
                table_def = table_info["table"]
                
                record = Record(table_def.all_fields, table_def.key_field)
                record.set_values(
                    sale_id=sale_id,
                    product_name=product_name,
                    quantity=quantity,
                    unit_price=unit_price,
                    sale_date=sale_date
                )
                
                result = db_manager.insert(table_name, record)
                count += 1
                
                if count % 20 == 0:
                    print(f"  Loaded {count} records | Total R/W: {result.disk_reads}/{result.disk_writes}")
                    
            except Exception as e:
                print(f"  Error loading row: {e}")
                continue
    
    print(f"\nTotal records loaded: {count}")
    return count

def test_secondary_index_search(db_manager, table_name):
    print(f"\n{'='*60}")
    print("SECONDARY INDEX SEARCH TEST (Two-Step Lookup)")
    print(f"{'='*60}")
    
    # Test 1: Search by product name
    print("\n1. Search by product_name='Drone':")
    print("   Step 1: Search unclustered index (product_name -> sale_id)")
    print("   Step 2: Search clustered index (sale_id -> full record)")
    result = db_manager.search(table_name, b"Drone", field_name="product_name")
    if result.data:
        print(f"   Found {len(result.data)} record(s):")
        for rec in result.data:
            print(f"     - ID:{rec.sale_id} | {rec.product_name} | Qty:{rec.quantity} | ${rec.unit_price}")
    else:
        print("   No records found")
    print(f"   Total Performance: {result.execution_time_ms:.2f}ms | Disk R/W: {result.disk_reads}/{result.disk_writes}")
    
    print("\n2. Search by product_name='NonExistent':")
    result = db_manager.search(table_name, b"NonExistent", field_name="product_name")
    print(f"   Found: {len(result.data)} records")
    print(f"   Performance: {result.execution_time_ms:.2f}ms | Disk R/W: {result.disk_reads}/{result.disk_writes}")
    
    print("\n3. Range search by product_name ('A' to 'C'):")
    result = db_manager.range_search(table_name, b"A", b"C", field_name="product_name")
    print(f"   Found: {len(result.data)} records")
    print("   Sample results:")
    for rec in result.data[:5]:
        print(f"     - {rec.product_name} (ID: {rec.sale_id})")
    if len(result.data) > 5:
        print(f"     ... and {len(result.data) - 5} more")
    print(f"   Performance: {result.execution_time_ms:.2f}ms | Disk R/W: {result.disk_reads}/{result.disk_writes}")

def test_secondary_index_delete(db_manager, table_name):
    print(f"\n{'='*60}")
    print("SECONDARY INDEX DELETE TEST")
    print(f"{'='*60}")
    
    print("\n1. Delete all records with product_name='Drone':")
    result = db_manager.delete(table_name, b"Drone", field_name="product_name")
    print(f"   Deleted: {result.data} record(s)")
    print(f"   Performance: {result.execution_time_ms:.2f}ms | Disk R/W: {result.disk_reads}/{result.disk_writes}")
    
    search_result = db_manager.search(table_name, b"Drone", field_name="product_name")
    print(f"   Verification: Found {len(search_result.data)} records (should be 0)")

def test_direct_vs_secondary_search(db_manager, table_name):
    print(f"\n{'='*60}")
    print("COMPARISON: Direct vs Secondary Index Search")
    print(f"{'='*60}")
    
    result = db_manager.search(table_name, b"Telescopio Digital", field_name="product_name")
    if result.data:
        sale_id = result.data[0].sale_id
        
        print(f"\nSearching for sale_id={sale_id} (Telescopio Digital)")
        
        print("\n  A) Direct search by sale_id (Primary/Clustered Index):")
        direct_result = db_manager.search(table_name, sale_id)
        print(f"     Single-step lookup")
        print(f"     Performance: {direct_result.execution_time_ms:.2f}ms | Disk R/W: {direct_result.disk_reads}/{direct_result.disk_writes}")
        
        print("\n  B) Search by product_name (Secondary -> Primary):")
        secondary_result = db_manager.search(table_name, b"Telescopio Digital", field_name="product_name")
        print(f"     Two-step lookup (unclustered -> clustered)")
        print(f"     Performance: {secondary_result.execution_time_ms:.2f}ms | Disk R/W: {secondary_result.disk_reads}/{secondary_result.disk_writes}")
        
        print(f"\n  Analysis:")
        print(f"    - Direct search: Faster (single index lookup)")
        print(f"    - Secondary search: Slower (two lookups required)")
        print(f"    - Direct R/W: {direct_result.disk_reads}/{direct_result.disk_writes}")
        print(f"    - Secondary R/W: {secondary_result.disk_reads}/{secondary_result.disk_writes}")
        print(f"    - Both retrieve the same record")
    else:
        print("   Product 'Telescopio Digital' not found in database")

def main():
    print("\n" + "="*60)
    print("B+ TREE UNCLUSTERED INDEX TEST - DISK-BASED")
    print("="*60)
    
    db_manager = DatabaseManager("btree_unclustered_test_db")
    table = create_sales_table()
    csv_path = "data/datasets/sales_dataset_unsorted.csv"
    
    print("\nCreating table 'sales' with B+ Tree Clustered Index (Primary)...")
    db_manager.create_table(table, primary_index_type="BTREE")
    print("[OK] Table created with B+ Tree (Clustered)")
    
    load_sales_from_csv(db_manager, "sales", csv_path, max_records=100)
    
    print(f"\n{'='*60}")
    print("Creating Secondary B+ Tree Index on 'product_name'...")
    print(f"{'='*60}")
    db_manager.create_index("sales", "product_name", "BTREE", scan_existing=True)
    print("[OK] Secondary index created and populated")
    print("  Index type: B+ Tree Unclustered (stores product_name -> sale_id)")
    
    test_secondary_index_search(db_manager, "sales")
    test_direct_vs_secondary_search(db_manager, "sales")
    test_secondary_index_delete(db_manager, "sales")
    

if __name__ == "__main__":
    main()