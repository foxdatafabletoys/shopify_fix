export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[];

export interface Database {
  public: {
    Tables: {
      conversation_runs: {
        Row: {
          id: string;
          user_id: string;
          mode: "onboarding" | "update";
          elevenlabs_conversation_id: string | null;
          status: string;
          raw_transcript: Json | null;
          processed_summary: string | null;
          target_doc_id: string | null;
          target_tab_id: string | null;
          error_message: string | null;
          created_at: string;
          updated_at: string;
        };
        Insert: {
          id?: string;
          user_id: string;
          mode: "onboarding" | "update";
          elevenlabs_conversation_id?: string | null;
          status?: string;
          raw_transcript?: Json | null;
          processed_summary?: string | null;
          target_doc_id?: string | null;
          target_tab_id?: string | null;
          error_message?: string | null;
          created_at?: string;
          updated_at?: string;
        };
        Update: Partial<
          Database["public"]["Tables"]["conversation_runs"]["Insert"]
        >;
      };
      profiles: {
        Row: {
          id: string;
          email: string | null;
          full_name: string | null;
          company_name: string | null;
          role_title: string | null;
          onboarding_doc_id: string | null;
          onboarding_doc_url: string | null;
          updates_tab_id: string | null;
          created_at: string;
          updated_at: string;
        };
        Insert: {
          id: string;
          email?: string | null;
          full_name?: string | null;
          company_name?: string | null;
          role_title?: string | null;
          onboarding_doc_id?: string | null;
          onboarding_doc_url?: string | null;
          updates_tab_id?: string | null;
          created_at?: string;
          updated_at?: string;
        };
        Update: Partial<Database["public"]["Tables"]["profiles"]["Insert"]>;
      };
    };
  };
}
