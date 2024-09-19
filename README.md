pathkvs
---

banco chave valor persistente transacionado baseado em operações atomicas com histórico

isso é uma prova de conceito demonstando um banco relativamente simples, que suporta ACID e detecta conflitos sem usar semáforos

o acrónimo significa *Persitent Atomic Transacted Historical Key Value Store*

## Como executar

primeito, tenha rust instalado, então:

* execute `cargo run serve`, para rodar um servidor na porta 6314
* execute `cargo run stress`, para incrementar a variável `INC` 500 vezes
* execute `cargo run`, para ter um terminal interativo com o qual você pode rodar comandos

## Testando o banco

depois de ligar o servidor com `cargo run serve`

tente executar `cargo run stress` multiplas vezes para ver eles lutando entre si tentando incrementar o `INC`

`cargo run stress` vai relatar quando um commit falhar por causa de um conflito

depois execute `cargo run`, e então digite `INC` e então aperte `Enter`, para ver o valor da variável `INC`

## Comandos do terminal interatico
1. `<chave>=<valor>` - salvar um valor no banco
2. `<chave>` - ler o valor da chave
3. `<comeco>*<fim>` - listar todas as chaves que começam com `<comeco>` e terminam com `<fim>`
3. `<comeco>*<fim>=` - mesma coisa que o comando acima, mas também mostra o valor
4. `commit` - comitar as mudanças
5. `rollback` - desfazer as mudanças
6. Ctrl + C - encerrar o programa

### Performance
é terrível, em uma máquina boa, mais ou menos 40ms por transação, 25 transações por segundo

isso é por causa da necessidade de sincronizar com o disco

### Features e caracteristicas
* suporta apenas isolamento serializável, que o nível mais alto que tem em bancos de dados
* têm um protocolo tcp/ip simples
* usa exclusivamente operações atomicas para resolver conflitos
* usa apenas um mutex para proteger a serialização do arquivo
* guarda todo o histórico de mudanças
* não é possível diminuir o tamanho do banco
* não suporta esperas na leitura, não é possível esperar até que um recurso esteja "liberado" (isso é uma grande limitação)
* não consegue ler do disco para evitar uso desnecessário de memória, todo o banco tem que caber na RAM
* não tem índices

### Suporta ACID
* *Atomicity* - caso o commit não seja escrito completamente ao banco, ele será ignorado quando o servidor for reiniciado, e quem fez o commit com certeza não receberá um ok
* *Consistency* - commits que entrem em conflito com outros commits já feitos não podem ser comitados
* *Isolation* - transações não podem ler modificações de outras transações que comitaram depois que a transação foi aberta
* *Durability* - mesma coisa que *Atomicity*, provavelmente foi adicionado para completar o acrónimo

### Conflitos
um conflito ocorre quando um commit faz leituras, e no meio tempo outros commits foram feitos que escrevem esse valores

como você pode estar dependendo de alguma invariante que faz as escritas dependendo dos valores que você leu,
implicitamente todas as escritas dependem das leituras na mesma transação

portanto, como um chute conservador, o sistema se recusa a comitar uma transação que leu valores que mudaram desde que a transação foi iniciada

isso significa que você tem que retentar a transação nesse caso, isso é uma limitação inerente de usar operações atomicas para resolver conflitos

### Como funciona
o código interessante está em `pathkvs-core/src/lib.rs`, tudo relacionado aos commits, conflitos, escritas, versionamento, histórico, serialização, está nesse arquivo

o resto é interface com o usuário, o protocolo tcp/ip, ou modelagem de erros

tudo gira em torno do membro `master` da struct `Database`, que é um ponteiro que só pode ser lido e escrito com operações atomicas

esse ponteiro aponta para um `Commit` que é um objeto imutável que contém todos os dados do banco

alterações ao banco são feitas criando um novo commit com as novas mudanças, e com um ponteiro para o commit anterior, e então escrevendo um novo ponteiro na `master`

quando uma thread começa uma transação, ela obtém uma cópia do ponteiro, como o conteúdo dele é imutável, se a thread apenas ler desse objeto ela vai sempre ler um estado consistente do banco, do momento em que ela iniciou a transação

para escrever ela simplesmente guarda em uma variável própia da transação quais chaves e valores ela pretende adicionar no commit que ela vai criar

para comitar, a thread cria o objeto commit e coloca o ponteiro do commit que ela estava lendo até agora como o commit anterior, é como se ela tivesse dado um `git commit` na branch local dela

então ela faz uma operação atomica chamada [*compare and swap*](https://en.wikipedia.org/wiki/Compare-and-swap), que vai colocar o commit dela como o último commit, caso o commit não tenha mudado no meio tempo, isso é o equivalente a um `git push` que deu certo

se não tiver mudado nada essa operação vai dar certo e commit vai estar na `master` e com o seu commit "aprovado" na memória, a thread parte para serializar o commit no disco

se alguma outra thread tiver commitado, o [*compare and swap*](https://en.wikipedia.org/wiki/Compare-and-swap) vai falhar e não vai mexer na master, isso é o equivalente a um `git push` que falhou por que alguem fez push enquanto você não estava olhando

o que você faz quando o `git push` não dá certo?

você puxa as últimas mudanças e merge elas as suas, e é isso que a thread então faz

mergir nesse contexto significa simplesmente atualizar o ponteiro do último commit no novo commit e verificar se nenhuma escrita ocorreu nos valores que nós tivemos que ler

caso uma escrita tenha ocorrido, a thread desiste por que ocorreu um conflito,

senão ela atualiza o ponteiro do commit anterior no seu novo commit para apontar para a nova master e tentar fazer o [*compare and swap*](https://en.wikipedia.org/wiki/Compare-and-swap) novamente, e isso se repete até que ela consiga comitar, ou alguem faça escritas que conflitem com as leituras desta thread

note que em nenhuma hora a thread é bloqueada por causa de um mutex ou lock, isso diminui a latências da resposta, pois não precisamo esperar que recursos fique disponíveis

ainda existe o mutex da serialização que todas as threads tem que usar depois que comitam para salvar suas mudanças no disco e poder retornar, e apesar de ser um mutex que todas as threads tem que usar, o trabalho da seção crítica é relativamente pequeno, e traz vários benefícios como a serialização dos commits e a preservação do histórico

esse modelo, tem altíssima [contenção](https://en.wikipedia.org/wiki/Resource_contention) na hora de comitar, isso se dá especialemente pelo fato de ter apenas um local onde se pode escrever as operações, fora esso situação as threads tem zero [contenção](https://en.wikipedia.org/wiki/Resource_contention), toda outra operação, iniciar transação, escritas, leituras e rollbacks são virtualmente gratuitas e completamente livres de [contenção](https://en.wikipedia.org/wiki/Resource_contention), limitadas apenas pela ram e cpu disponível no computador

e um fato interessante é que nesse modelo um rollback, não dá trabalho nenhum, é só a thread esqueçer o ponteiro para o commit e as mudanças e leituras que estava rastreando

### Persistência
o arquivo do banco de dados é um banco *append-only* que guarda todas as mudanças feitas no banco, se adicionarmos metadados aos commits como quem fez e quando fez (algo que a implementação hoje não faz), é possível voltar no tempo e fazer perguntas sobre como os dados estavam antes de um certo tempo, ou antes de uma certa mudança

isso também tem implicações quanto aos backups, que não seria necessário guardar múltiplos backups diários, pois isso iria estar guardando o histórico multiplas vezes no mesmo disco, seria melhor tem uma cópia em cada ponto de falha (discos), e apenas copiar o novo histórico para cada um, pois, se o que você quer é ver como o banco estava no passado, isso estaria presente no banco principal e não teria necessidade de apelar para backups
