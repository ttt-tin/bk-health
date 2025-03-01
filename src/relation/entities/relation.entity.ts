import { Entity, PrimaryGeneratedColumn, Column } from "typeorm";

@Entity("relationships")
export class RelationshipEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ name: "table_reference", type: "varchar", length: 255 })
  tableReference: string;

  @Column({ name: "table_was_reference", type: "varchar", length: 255 })
  tableWasReference: string;

  @Column({ name: "pri_key", type: "varchar", length: 255 })
  priKey: string;

  @Column({ name: "fo_key", type: "varchar", length: 255 })
  foKey: string;
}
