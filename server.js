create table if not exists pedidos (
  id bigint generated always as identity primary key,
  t timestamptz not null default now(),
  ip text not null,
  item text not null,
  quantidade integer not null check (quantidade between 1 and 500),
  nivel text not null check (nivel in ('OK','ATENCAO','CRITICO')),
  simulado boolean not null default false
);

create index if not exists idx_pedidos_t on pedidos (t desc);
create index if not exists idx_pedidos_ip_t on pedidos (ip, t desc);

create table if not exists db_metricas (
  id bigint generated always as identity primary key,
  t timestamptz not null default now(),
  tamanho_mb numeric(10,2) not null default 0,
  paginas bigint not null default 0,
  paginas_livres bigint not null default 0,
  wal_mb numeric(10,2) not null default 0,
  escritas bigint not null default 0,
  leituras bigint not null default 0,
  ms_ultima_escrita numeric(10,3) not null default 0,
  ms_ultima_leitura numeric(10,3) not null default 0
);

create index if not exists idx_db_metricas_t on db_metricas (t desc);

alter table pedidos enable row level security;
alter table db_metricas enable row level security;

drop policy if exists "pedidos_leitura_publica" on pedidos;
create policy "pedidos_leitura_publica" on pedidos for select using (true);

drop policy if exists "pedidos_insert_publico" on pedidos;
create policy "pedidos_insert_publico" on pedidos for insert with check (true);

drop policy if exists "pedidos_delete_publico" on pedidos;
create policy "pedidos_delete_publico" on pedidos for delete using (true);

drop policy if exists "db_metricas_leitura_publica" on db_metricas;
create policy "db_metricas_leitura_publica" on db_metricas for select using (true);

drop policy if exists "db_metricas_insert_publico" on db_metricas;
create policy "db_metricas_insert_publico" on db_metricas for insert with check (true);

drop policy if exists "db_metricas_delete_publico" on db_metricas;
create policy "db_metricas_delete_publico" on db_metricas for delete using (true);

create or replace function limpar_pedidos_antigos()
returns void language sql as $$
  delete from pedidos where t < now() - interval '10 minutes';
  delete from db_metricas where t < now() - interval '30 minutes';
$$;

create or replace function pedidos_por_ip_janela(segundos integer default 60)
returns table (ip text, pedidos bigint, unidades bigint, ultimo timestamptz, simulado boolean, nivel text)
language sql stable as $$
  select
    ip,
    count(*) as pedidos,
    coalesce(sum(quantidade),0)::bigint as unidades,
    max(t) as ultimo,
    bool_or(simulado) as simulado,
    case
      when count(*) >= 10 then 'CRITICO'
      when count(*) >= 5 then 'ATENCAO'
      else 'OK'
    end as nivel
  from pedidos
  where t >= now() - (segundos || ' seconds')::interval
  group by ip
  order by pedidos desc, unidades desc
  limit 10;
$$;

create or replace function unidades_na_janela(segundos integer default 60)
returns bigint language sql stable as $$
  select coalesce(sum(quantidade),0)::bigint
  from pedidos
  where t >= now() - (segundos || ' seconds')::interval;
$$;

create or replace function contar_pedidos_ip(p_ip text, segundos integer default 60)
returns bigint language sql stable as $$
  select count(*)::bigint
  from pedidos
  where ip = p_ip and t >= now() - (segundos || ' seconds')::interval;
$$;

create or replace function metricas_banco()
returns table (tamanho_mb numeric, paginas bigint, paginas_livres bigint, wal_mb numeric)
language sql stable as $$
  select
    round(pg_database_size(current_database())::numeric / 1048576, 2) as tamanho_mb,
    (select count(*) from pg_stat_activity)::bigint as paginas,
    0::bigint as paginas_livres,
    coalesce((select sum(size) from pg_ls_waldir())::numeric / 1048576, 0)::numeric(10,2) as wal_mb;
$$;
